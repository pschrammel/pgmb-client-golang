package main

import (
    "context"
    "encoding/json"
    "log"

    "github.com/yourusername/pgmb-client-golang"
)

// Define your message types
type UserRegistrationEvent struct {
    UserID   string `json:"user_id"`
    Email    string `json:"email"`
    Username string `json:"username"`
}

type OrderCreatedEvent struct {
    OrderID    string  `json:"order_id"`
    UserID     string  `json:"user_id"`
    TotalAmount float64 `json:"total_amount"`
}

// Helper functions for type-safe message handling
func SendTypedMessage[T any](ctx context.Context, client *pgmb.Client, queueName string, data T, headers map[string]any) error {
    messageBytes, err := json.Marshal(data)
    if err != nil {
        return err
    }

    _, err = client.Send(ctx, queueName, pgmb.SendMessageOptions{
        Message: messageBytes,
        Headers: headers,
    })
    return err
}

func PublishTypedMessage[T any](ctx context.Context, client *pgmb.Client, exchange string, data T, headers map[string]any) error {
    messageBytes, err := json.Marshal(data)
    if err != nil {
        return err
    }

    _, err = client.Publish(ctx, pgmb.PublishMessageOptions{
        Exchange: exchange,
        Message:  messageBytes,
        Headers:  headers,
    })
    return err
}

func ProcessTypedMessages[T any](messages []pgmb.Message, ack pgmb.AckFunc, processor func(T, string) error) {
    for _, msg := range messages {
        var data T
        if err := json.Unmarshal(msg.Message, &data); err != nil {
            log.Printf("Error unmarshaling message %s: %v", msg.ID, err)
            ack(false, msg.ID)
            continue
        }

        if err := processor(data, msg.ID); err != nil {
            log.Printf("Error processing message %s: %v", msg.ID, err)
            ack(false, msg.ID)
        } else {
            ack(true, msg.ID)
        }
    }
}

func main() {
    ctx := context.Background()
    
    client, err := pgmb.NewClient(ctx, pgmb.ClientOptions{
        DSN: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable",
        Consumers: []pgmb.ConsumerOptions{
            {
                Name: "user_registration_queue",
                OnMessage: func(ctx context.Context, queueName string, messages []pgmb.Message, ack pgmb.AckFunc) error {
                    ProcessTypedMessages(messages, ack, func(event UserRegistrationEvent, messageID string) error {
                        log.Printf("Processing user registration: %+v", event)
                        // Handle user registration...
                        return nil
                    })
                    return nil
                },
            },
            {
                Name: "order_processing_queue",
                OnMessage: func(ctx context.Context, queueName string, messages []pgmb.Message, ack pgmb.AckFunc) error {
                    ProcessTypedMessages(messages, ack, func(event OrderCreatedEvent, messageID string) error {
                        log.Printf("Processing order: %+v", event)
                        // Handle order processing...
                        return nil
                    })
                    return nil
                },
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Setup queues
    queues := []string{"user_registration_queue", "order_processing_queue"}
    for _, queue := range queues {
        err = client.AssertQueue(ctx, pgmb.QueueOptions{
            Name: queue,
        })
        if err != nil {
            log.Fatal(err)
        }
    }

    // Start consuming
    if err := client.Listen(); err != nil {
        log.Fatal(err)
    }

    // Send typed messages
    userEvent := UserRegistrationEvent{
        UserID:   "user123",
        Email:    "user@example.com",
        Username: "john_doe",
    }

    err = SendTypedMessage(ctx, client, "user_registration_queue", userEvent, map[string]any{
        "event_type": "user_registration",
        "timestamp":  time.Now().Unix(),
    })
    if err != nil {
        log.Fatal(err)
    }

    orderEvent := OrderCreatedEvent{
        OrderID:     "order456",
        UserID:      "user123",
        TotalAmount: 99.99,
    }

    err = SendTypedMessage(ctx, client, "order_processing_queue", orderEvent, map[string]any{
        "event_type": "order_created",
        "timestamp":  time.Now().Unix(),
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Typed messages sent successfully!")

    // Keep running
    select {}
}
