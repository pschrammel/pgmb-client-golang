# PGMB Go Client

A Go client library for [PGMB](https://github.com/haathie/pgmb) - a PostgreSQL-based message broker.

## Features

- **Queues**: FIFO queues with exactly-once delivery and multiple consumer support
- **Exchanges**: Route messages to one or more queues
- **Fanout Bindings**: All queues bound to an exchange receive all messages
- **Retries**: Configurable retry mechanisms
- **Real-time Consumption**: Using PostgreSQL's LISTEN/NOTIFY
- **Multiple Consumers**: Multiple consumers can consume from the same queue
- **Scheduled Messages**: Schedule messages for future consumption
- **Bulk Operations**: Publish and consume messages in bulk
- **Message Batching**: Efficient batching for high-throughput scenarios
- **Type Safety**: Strong typing support with generics

## Installation

```bash
go get github.com/pschrammel/pgmb-client-golang
```

## Prerequisites

Before using this client, you must install PGMB in your PostgreSQL database:

```bash
psql postgres://user:pass@host:port/db -f sql/pgmb.sql -1
```

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "log"

    "github.com/pschrammel/pgmb-client-golang"
)

func main() {
    ctx := context.Background()
    
    // Create client
    client, err := pgmb.NewClient(ctx, pgmb.ClientOptions{
        DSN: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Create a queue
    err = client.AssertQueue(ctx, pgmb.QueueOptions{
        Name: "my_queue",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Send a message
    messages, err := client.Send(ctx, "my_queue", pgmb.SendMessageOptions{
        Message: []byte("Hello, World!"),
        Headers: map[string]any{"source": "example"},
    })
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Published message with ID: %s", messages[0].ID)
}
```

Consumer sample

```go
package main

import (
    "context"
    "log"

    "github.com/pschrammel/pgmb-client-golang"
)

func main() {
    ctx := context.Background()
    
    // Create client with consumers
    client, err := pgmb.NewClient(ctx, pgmb.ClientOptions{
        DSN: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable",
        Consumers: []pgmb.ConsumerOptions{
            {
                Name:      "my_queue",
                BatchSize: 10,
                OnMessage: func(ctx context.Context, queueName string, messages []pgmb.Message, ack pgmb.AckFunc) error {
                    for _, msg := range messages {
                        log.Printf("Received message: %s", string(msg.Message))
                        ack(true, msg.ID) // Acknowledge success
                    }
                    return nil
                },
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Start listening
    if err := client.Listen(); err != nil {
        log.Fatal(err)
    }

    // Keep running
    select {}
}
```

