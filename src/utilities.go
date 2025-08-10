package pgmb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// MessageSerializer interface for custom message serialization
type MessageSerializer interface {
	Serialize(data any) ([]byte, error)
	Deserialize(data []byte, target any) error
}

// JSONSerializer implements MessageSerializer using JSON
type JSONSerializer struct{}

func (j JSONSerializer) Serialize(data any) ([]byte, error) {
	return json.Marshal(data)
}

func (j JSONSerializer) Deserialize(data []byte, target any) error {
	return json.Unmarshal(data, target)
}

// TypedClient provides type-safe operations
type TypedClient[T any] struct {
	client     *Client
	serializer MessageSerializer
}

// NewTypedClient creates a new typed client wrapper
func NewTypedClient[T any](client *Client, serializer MessageSerializer) *TypedClient[T] {
	if serializer == nil {
		serializer = JSONSerializer{}
	}
	
	return &TypedClient[T]{
		client:     client,
		serializer: serializer,
	}
}

// SendTyped sends a typed message to a queue
func (tc *TypedClient[T]) SendTyped(ctx context.Context, queueName string, data T, headers map[string]any) ([]PublishedMessage, error) {
	messageBytes, err := tc.serializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize message: %w", err)
	}

	return tc.client.Send(ctx, queueName, SendMessageOptions{
		Message: messageBytes,
		Headers: headers,
	})
}

// PublishTyped publishes a typed message to an exchange
func (tc *TypedClient[T]) PublishTyped(ctx context.Context, exchange string, data T, headers map[string]any) ([]PublishedMessage, error) {
	messageBytes, err := tc.serializer.Serialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize message: %w", err)
	}

	return tc.client.Publish(ctx, PublishMessageOptions{
		Exchange: exchange,
		Message:  messageBytes,
		Headers:  headers,
	})
}

// QueueMetrics represents queue statistics
type QueueMetrics struct {
	QueueName         string     `json:"queue_name"`
	PendingMessages   int64      `json:"pending_messages"`
	ProcessedMessages int64      `json:"processed_messages"`
	FailedMessages    int64      `json:"failed_messages"`
	LastActivity      *time.Time `json:"last_activity,omitempty"`
}

// GetQueueMetrics retrieves metrics for a specific queue
func (c *Client) GetQueueMetrics(ctx context.Context, queueName string) (*QueueMetrics, error) {
	query := `SELECT pgmb.get_queue_metrics($1)`
	
	var metricsJSON string
	err := c.db.QueryRowContext(ctx, query, queueName).Scan(&metricsJSON)
	if err != nil {
		return nil, err
	}

	var metrics QueueMetrics
	err = json.Unmarshal([]byte(metricsJSON), &metrics)
	if err != nil {
		return nil, err
	}

	return &metrics, nil
}

// HealthCheck performs a health check on the client
func (c *Client) HealthCheck(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if c.closed {
		return fmt.Errorf("client is closed")
	}

	// Test database connection
	if err := c.db.PingContext(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}

	// Test a simple query
	var result int
	err := c.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("database query failed: %w", err)
	}

	return nil
}
