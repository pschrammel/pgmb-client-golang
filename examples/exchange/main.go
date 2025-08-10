package main

import (
    "context"
    "encoding/json"
    "log"

    "github.com/pschrammel/pgmb-client-golang"
)

type UserEvent struct {
    UserID    string `json:"user_id"`
    EventType string `json:"event_type"`
    Timestamp int64  `json:"timestamp"`
    Data      any    `json:"data"`
}

func main() {
    ctx := context.Background()
    
    client, err := pgmb.NewClient(ctx, pgmb.ClientOptions{
        DSN: "postgres://postgres:password@localhost:5432/mydb?sslmode=disable",
        Consumers: []pgmb.ConsumerOptions{
            {
                Name: "email_service",
                OnMessage: func(ctx context.Context, queueName string, messages []pgmb.Message, ack pgmb.AckFunc) error {
                    for _, msg := range messages {
                        var event UserEvent
                        if err := json.Unmarshal(msg.Message, &event); err != nil {
                            log.Printf("Error unmarshaling message: %v", err)
                            ack(false, msg.ID)
                            continue
                        }
                        
                        log.Printf("Email service: Processing %s event for user %s", event.EventType, event.UserID)
                        ack(true, msg.ID)
                    }
                    return nil
                },
            },
            {
                Name: "analytics_service",
                OnMessage: func(ctx context.Context, queueName string, messages []pgmb.Message, ack pgmb.AckFunc) error {
                    for _, msg := range messages {
                        var event UserEvent
                        if err := json.Unmarshal(msg.Message, &event); err != nil {
                            log.Printf("Error unmarshaling message: %v", err)
                            ack(false, msg.ID)
                            continue
                        }
                        
                        log.Printf("Analytics service: Recording %s event for user %s", event.EventType, event.UserID)
                        ack(true, msg.ID)
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

    // Setup exchange and queues
    err = client.AssertExchange(ctx, pgmb.ExchangeOptions{Name: "user_events"})
    if err != nil {
        log.Fatal(err)
    }

    services := []string{"email_service", "analytics_service"}
    for _, service := range services {
        err = client.AssertQueue(ctx, pgmb.QueueOptions{
            Name:     service,
            Bindings: []string{"user_events"},
        })
        if err != nil {
            log.Fatal(err)
        }
        
        err = client.BindQueue(ctx, service, "user_events")
        if err != nil {
            log.Fatal(err)
        }
    }

    // Start consuming
    if err := client.Listen(); err != nil {
        log.Fatal(err)
    }

    // Publish events
    events := []UserEvent{
        {
            UserID:    "user123",
            EventType: "registration",
            Timestamp: 1640995200,
            Data:      map[string]any{"email": "user@example.com"},
        },
        {
            UserID:    "user456",
            EventType: "login",
            Timestamp: 1640995260,
            Data:      map[string]any{"ip": "192.168.1.1"},
        },
    }

    for _, event := range events {
        eventJSON, _ := json.Marshal(event)
        
        messages, err := client.Publish(ctx, pgmb.PublishMessageOptions{
            Exchange: "user_events",
            Message:  eventJSON,
            Headers: map[string]any{
                "event_type": event.EventType,
                "user_id":    event.UserID,
            },
        })
        if err != nil {
            log.Fatal(err)
        }
        
        log.Printf("Published %s event for user %s (ID: %s)", event.EventType, event.UserID, messages[0].ID)
    }

    // Keep running
    select {}
}
