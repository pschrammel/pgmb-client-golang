package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/yourusername/pgmb-client-golang"
)

func main() {
    ctx := context.Background()
    
    // Create client with batcher
    client, err := pgmb.NewClient(ctx, pgmb.ClientOptions{
        DSN: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable",
        Batcher: &pgmb.BatcherOptions{
            FlushIntervalMs: 2000, // Auto-flush every 2 seconds
            MaxBatchSize:    50,   // Auto-flush when batch reaches 50 messages
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Create exchange and queue
    err = client.AssertExchange(ctx, pgmb.ExchangeOptions{
        Name: "batch_exchange",
    })
    if err != nil {
        log.Fatal(err)
    }

    err = client.AssertQueue(ctx, pgmb.QueueOptions{
        Name:     "batch_queue",
        Bindings: []string{"batch_exchange"},
    })
    if err != nil {
        log.Fatal(err)
    }

    batcher := client.GetDefaultBatcher()

    // Simulate high-frequency message generation
    go func() {
        for i := 0; i < 100; i++ {
            // Enqueue to exchange
            batcher.Enqueue(pgmb.PublishMessageOptions{
                Exchange: "batch_exchange",
                Message:  []byte(fmt.Sprintf("Exchange message %d", i)),
                Headers: map[string]any{
                    "batch_id":    "batch_001",
                    "message_num": i,
                },
            })

            // Enqueue directly to queue
            batcher.EnqueueToQueue("batch_queue", pgmb.SendMessageOptions{
                Message: []byte(fmt.Sprintf("Direct queue message %d", i)),
                Headers: map[string]any{
                    "direct":      true,
                    "message_num": i,
                },
            })

            time.Sleep(50 * time.Millisecond)
        }
    }()

    // Wait and then manually flush
    time.Sleep(3 * time.Second)
    
    log.Println("Manually flushing remaining messages...")
    err = batcher.Flush(ctx)
    if err != nil {
        log.Fatal(err)
    }

    log.Println("All messages sent successfully!")
}
