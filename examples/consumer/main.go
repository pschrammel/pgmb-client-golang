package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/pschrammel/pgmb-client-golang"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Handle graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigChan
        log.Println("Shutting down...")
        cancel()
    }()

    // Create client with consumer
    client, err := pgmb.NewClient(ctx, pgmb.ClientOptions{
        DSN: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable",
        Consumers: []pgmb.ConsumerOptions{
            {
                Name:               "work_queue",
                BatchSize:          5,
                DebounceIntervalMs: 1000,
                Replicas:           2,
                OnMessage: func(ctx context.Context, queueName string, messages []pgmb.Message, ack pgmb.AckFunc) error {
                    log.Printf("Processing batch of %d messages from %s", len(messages), queueName)
                    
                    for _, msg := range messages {
                        log.Printf("Processing message %s: %s", msg.ID, string(msg.Message))
                        
                        if string(msg.Message) == "fail" {
                            ack(false, msg.ID)
                        } else {
                            ack(true, msg.ID)
                        }
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

    log.Println("Consumer started, waiting for messages...")
    
    // Wait for shutdown signal
    <-ctx.Done()
    log.Println("Consumer stopped")
}
