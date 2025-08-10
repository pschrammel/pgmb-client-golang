package main

import (
    "context"
    "log"

    "github.com/yourusername/pgmb-client-golang"
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
        Name:       "simple_queue",
        AckSetting: "delete",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Send messages
    messages, err := client.Send(ctx, "simple_queue",
        pgmb.SendMessageOptions{
            Message: []byte("Hello, World!"),
            Headers: map[string]any{"source": "example", "priority": 1},
        },
        pgmb.SendMessageOptions{
            Message: []byte("Second message"),
            Headers: map[string]any{"source": "example", "priority": 2},
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    for _, msg := range messages {
        log.Printf("Published message with ID: %s", msg.ID)
    }
}
