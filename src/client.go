package pgmb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

// Message represents a message in the queue
type Message struct {
	ID       string            `json:"id"`
	Message  []byte            `json:"message"`
	Headers  map[string]any    `json:"headers"`
	Exchange *string           `json:"exchange,omitempty"`
}

// PublishedMessage represents a successfully published message
type PublishedMessage struct {
	ID string `json:"id"`
}

// QueueOptions defines options for queue creation
type QueueOptions struct {
	Name           string            `json:"name"`
	AckSetting     string            `json:"ack_setting,omitempty"`     // "delete", "archive"
	Bindings       []string          `json:"bindings,omitempty"`
	Type           string            `json:"type,omitempty"`            // "logged", "unlogged"
	DefaultHeaders map[string]any    `json:"default_headers,omitempty"`
}

// ExchangeOptions defines options for exchange creation
type ExchangeOptions struct {
	Name string `json:"name"`
}

// SendMessageOptions defines options for sending messages
type SendMessageOptions struct {
	Message []byte         `json:"message"`
	Headers map[string]any `json:"headers,omitempty"`
}

// PublishMessageOptions defines options for publishing messages to exchanges
type PublishMessageOptions struct {
	Exchange string         `json:"exchange"`
	Message  []byte         `json:"message"`
	Headers  map[string]any `json:"headers,omitempty"`
}

// ConsumerOptions defines options for a consumer
type ConsumerOptions struct {
	Name                string                 `json:"name"`
	AckSetting          string                 `json:"ack_setting,omitempty"`
	Bindings            []string               `json:"bindings,omitempty"`
	Type                string                 `json:"type,omitempty"`
	BatchSize           int                    `json:"batch_size,omitempty"`
	DebounceIntervalMs  int                    `json:"debounce_interval_ms,omitempty"`
	Replicas            int                    `json:"replicas,omitempty"`
	OnMessage           MessageHandler         `json:"-"`
}

// MessageHandler is a function that processes messages
type MessageHandler func(ctx context.Context, queueName string, messages []Message, ack AckFunc) error

// AckFunc allows selective acknowledgment of messages
type AckFunc func(success bool, messageID string)

// BatcherOptions defines options for the message batcher
type BatcherOptions struct {
	FlushIntervalMs int `json:"flush_interval_ms,omitempty"`
	MaxBatchSize    int `json:"max_batch_size,omitempty"`
}

// ClientOptions defines options for the PGMB client
type ClientOptions struct {
	DB         *sql.DB              `json:"-"`
	DSN        string               `json:"dsn,omitempty"`
	Consumers  []ConsumerOptions    `json:"consumers,omitempty"`
	Batcher    *BatcherOptions      `json:"batcher,omitempty"`
}

// Client represents the PGMB client
type Client struct {
	db        *sql.DB
	consumers []ConsumerOptions
	batcher   *Batcher
	listener  *pq.Listener
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.RWMutex
	closed    bool
}

// Batcher handles batching of messages for efficient publishing
type Batcher struct {
	client          *Client
	messages        []PublishMessageOptions
	queueMessages   map[string][]SendMessageOptions
	mu              sync.Mutex
	flushInterval   time.Duration
	maxBatchSize    int
	ticker          *time.Ticker
	done            chan struct{}
}

// NewClient creates a new PGMB client
func NewClient(ctx context.Context, opts ClientOptions) (*Client, error) {
	var db *sql.DB
	var err error

	if opts.DB != nil {
		db = opts.DB
	} else if opts.DSN != "" {
		db, err = sql.Open("postgres", opts.DSN)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to database: %w", err)
		}
		
		if err = db.Ping(); err != nil {
			return nil, fmt.Errorf("failed to ping database: %w", err)
		}
	} else {
		return nil, fmt.Errorf("either DB or DSN must be provided")
	}

	clientCtx, cancel := context.WithCancel(ctx)
	
	client := &Client{
		db:        db,
		consumers: opts.Consumers,
		ctx:       clientCtx,
		cancel:    cancel,
	}

	// Setup batcher if configured
	if opts.Batcher != nil {
		client.batcher = &Batcher{
			client:        client,
			messages:      make([]PublishMessageOptions, 0),
			queueMessages: make(map[string][]SendMessageOptions),
			maxBatchSize:  opts.Batcher.MaxBatchSize,
			done:          make(chan struct{}),
		}
		
		if opts.Batcher.FlushIntervalMs > 0 {
			client.batcher.flushInterval = time.Duration(opts.Batcher.FlushIntervalMs) * time.Millisecond
			client.batcher.ticker = time.NewTicker(client.batcher.flushInterval)
		}
	}

	return client, nil
}

// AssertQueue creates or updates a queue
func (c *Client) AssertQueue(ctx context.Context, opts QueueOptions) error {
	query := `SELECT pgmb.assert_queue($1, $2, $3, $4, $5)`
	
	bindingsJSON, _ := json.Marshal(opts.Bindings)
	defaultHeadersJSON, _ := json.Marshal(opts.DefaultHeaders)
	
	ackSetting := opts.AckSetting
	if ackSetting == "" {
		ackSetting = "delete"
	}
	
	queueType := opts.Type
	if queueType == "" {
		queueType = "logged"
	}
	
	_, err := c.db.ExecContext(ctx, query, 
		opts.Name, 
		ackSetting, 
		string(bindingsJSON), 
		queueType, 
		string(defaultHeadersJSON),
	)
	return err
}

// AssertExchange creates or updates an exchange
func (c *Client) AssertExchange(ctx context.Context, opts ExchangeOptions) error {
	query := `SELECT pgmb.assert_exchange($1)`
	_, err := c.db.ExecContext(ctx, query, opts.Name)
	return err
}

// BindQueue binds a queue to an exchange
func (c *Client) BindQueue(ctx context.Context, queueName, exchangeName string) error {
	query := `SELECT pgmb.bind_queue($1, $2)`
	_, err := c.db.ExecContext(ctx, query, queueName, exchangeName)
	return err
}

// Send sends messages directly to a queue
func (c *Client) Send(ctx context.Context, queueName string, messages ...SendMessageOptions) ([]PublishedMessage, error) {
	if len(messages) == 0 {
		return []PublishedMessage{}, nil
	}
	
	var result []PublishedMessage
	
	if len(messages) == 1 {
		// Single message
		msg := messages[0]
		headersJSON, _ := json.Marshal(msg.Headers)
		
		query := `SELECT pgmb.send($1, $2, $3)`
		var id string
		err := c.db.QueryRowContext(ctx, query, queueName, msg.Message, string(headersJSON)).Scan(&id)
		if err != nil {
			return nil, err
		}
		result = append(result, PublishedMessage{ID: id})
	} else {
		// Bulk messages
		messagesData := make([]map[string]interface{}, len(messages))
		for i, msg := range messages {
			messagesData[i] = map[string]interface{}{
				"message": msg.Message,
				"headers": msg.Headers,
			}
		}
		
		messagesJSON, _ := json.Marshal(messagesData)
		query := `SELECT pgmb.send_bulk($1, $2)`
		
		rows, err := c.db.QueryContext(ctx, query, queueName, string(messagesJSON))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			result = append(result, PublishedMessage{ID: id})
		}
	}
	
	return result, nil
}

// Publish publishes messages to exchanges
func (c *Client) Publish(ctx context.Context, messages ...PublishMessageOptions) ([]PublishedMessage, error) {
	if len(messages) == 0 {
		return []PublishedMessage{}, nil
	}
	
	var result []PublishedMessage
	
	if len(messages) == 1 {
		// Single message
		msg := messages[0]
		headersJSON, _ := json.Marshal(msg.Headers)
		
		query := `SELECT pgmb.publish($1, $2, $3)`
		var id string
		err := c.db.QueryRowContext(ctx, query, msg.Exchange, msg.Message, string(headersJSON)).Scan(&id)
		if err != nil {
			return nil, err
		}
		result = append(result, PublishedMessage{ID: id})
	} else {
		// Bulk messages
		messagesData := make([]map[string]interface{}, len(messages))
		for i, msg := range messages {
			messagesData[i] = map[string]interface{}{
				"exchange": msg.Exchange,
				"message":  msg.Message,
				"headers":  msg.Headers,
			}
		}
		
		messagesJSON, _ := json.Marshal(messagesData)
		query := `SELECT pgmb.publish_bulk($1)`
		
		rows, err := c.db.QueryContext(ctx, query, string(messagesJSON))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			result = append(result, PublishedMessage{ID: id})
		}
	}
	
	return result, nil
}

// Listen starts listening for messages on all configured consumers
func (c *Client) Listen(retriesLeft ...int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return fmt.Errorf("client is closed")
	}
	
	// Assert all queues for consumers
	for _, consumer := range c.consumers {
		queueOpts := QueueOptions{
			Name:       consumer.Name,
			AckSetting: consumer.AckSetting,
			Bindings:   consumer.Bindings,
			Type:       consumer.Type,
		}
		
		if err := c.AssertQueue(c.ctx, queueOpts); err != nil {
			return fmt.Errorf("failed to assert queue %s: %w", consumer.Name, err)
		}
	}
	
	// Setup listener
	listener := pq.NewListener(c.getDSN(), 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("PGMB listener error: %v", err)
		}
	})
	
	c.listener = listener
	
	// Listen on all consumer queues
	for _, consumer := range c.consumers {
		if err := listener.Listen("pgmb_" + consumer.Name); err != nil {
			return fmt.Errorf("failed to listen on queue %s: %w", consumer.Name, err)
		}
		
		// Start consumer goroutines
		replicas := consumer.Replicas
		if replicas <= 0 {
			replicas = 1
		}
		
		for i := 0; i < replicas; i++ {
			c.wg.Add(1)
			go c.consumeMessages(consumer)
		}
	}
	
	// Start batcher if configured
	if c.batcher != nil && c.batcher.ticker != nil {
		c.wg.Add(1)
		go c.batcher.run()
	}
	
	// Listen for notifications
	c.wg.Add(1)
	go c.listenForNotifications()
	
	return nil
}

func (c *Client) getDSN() string {
	// This is a simplified approach - in practice, you'd want to store the DSN
	// or reconstruct it from the connection parameters
	return "postgres://localhost/postgres"
}

func (c *Client) listenForNotifications() {
	defer c.wg.Done()
	
	for {
		select {
		case <-c.ctx.Done():
			return
		case notification := <-c.listener.Notify:
			if notification != nil {
				// Notification received, consumers will handle it
				_ = notification
			}
		case <-time.After(90 * time.Second):
			// Ping to keep connection alive
			if err := c.listener.Ping(); err != nil {
				log.Printf("PGMB listener ping error: %v", err)
			}
		}
	}
}

func (c *Client) consumeMessages(consumer ConsumerOptions) {
	defer c.wg.Done()
	
	batchSize := consumer.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}
	
	debounceInterval := time.Duration(consumer.DebounceIntervalMs) * time.Millisecond
	if debounceInterval <= 0 {
		debounceInterval = 100 * time.Millisecond
	}
	
	ticker := time.NewTicker(debounceInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			messages, err := c.fetchMessages(consumer.Name, batchSize)
			if err != nil {
				log.Printf("Error fetching messages from %s: %v", consumer.Name, err)
				continue
			}
			
			if len(messages) == 0 {
				continue
			}
			
			if consumer.OnMessage != nil {
				acks := make(map[string]bool)
				ackFunc := func(success bool, messageID string) {
					acks[messageID] = success
				}
				
				err := consumer.OnMessage(c.ctx, consumer.Name, messages, ackFunc)
				
				// Process acknowledgments
				for _, msg := range messages {
					success, exists := acks[msg.ID]
					if !exists {
						// Default to success if not explicitly acked/nacked
						success = err == nil
					}
					
					if success {
						c.ackMessage(msg.ID)
					} else {
						c.nackMessage(msg.ID)
					}
				}
			}
		}
	}
}

func (c *Client) fetchMessages(queueName string, batchSize int) ([]Message, error) {
	query := `SELECT pgmb.consume($1, $2)`
	
	rows, err := c.db.QueryContext(c.ctx, query, queueName, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var messages []Message
	for rows.Next() {
		var messageData string
		if err := rows.Scan(&messageData); err != nil {
			return nil, err
		}
		
		var msg Message
		if err := json.Unmarshal([]byte(messageData), &msg); err != nil {
			return nil, err
		}
		
		messages = append(messages, msg)
	}
	
	return messages, nil
}

func (c *Client) ackMessage(messageID string) error {
	query := `SELECT pgmb.ack($1)`
	_, err := c.db.ExecContext(c.ctx, query, messageID)
	return err
}

func (c *Client) nackMessage(messageID string) error {
	query := `SELECT pgmb.nack($1)`
	_, err := c.db.ExecContext(c.ctx, query, messageID)
	return err
}

// GetDefaultBatcher returns the default batcher instance
func (c *Client) GetDefaultBatcher() *Batcher {
	return c.batcher
}

// Close closes the client and all its resources
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return nil
	}
	
	c.closed = true
	
	// Flush batcher if exists
	if c.batcher != nil {
		c.batcher.Flush(c.ctx)
		if c.batcher.ticker != nil {
			c.batcher.ticker.Stop()
		}
		close(c.batcher.done)
	}
	
	// Cancel context and wait for goroutines
	c.cancel()
	c.wg.Wait()
	
	// Close listener
	if c.listener != nil {
		c.listener.Close()
	}
	
	return nil
}

// Batcher methods

// Enqueue adds a message to the batch for publishing to an exchange
func (b *Batcher) Enqueue(msg PublishMessageOptions) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	b.messages = append(b.messages, msg)
	
	if b.maxBatchSize > 0 && len(b.messages) >= b.maxBatchSize {
		go func() {
			_ = b.Flush(context.Background())
		}()
	}
}

// EnqueueToQueue adds a message to the batch for sending to a queue
func (b *Batcher) EnqueueToQueue(queueName string, msg SendMessageOptions) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if b.queueMessages[queueName] == nil {
		b.queueMessages[queueName] = make([]SendMessageOptions, 0)
	}
	
	b.queueMessages[queueName] = append(b.queueMessages[queueName], msg)
	
	totalMessages := len(b.messages)
	for _, msgs := range b.queueMessages {
		totalMessages += len(msgs)
	}
	
	if b.maxBatchSize > 0 && totalMessages >= b.maxBatchSize {
		go func() {
			_ = b.Flush(context.Background())
		}()
	}
}

// Flush publishes all batched messages
func (b *Batcher) Flush(ctx context.Context) error {
	b.mu.Lock()
	messages := make([]PublishMessageOptions, len(b.messages))
	copy(messages, b.messages)
	b.messages = b.messages[:0]
	
	queueMessages := make(map[string][]SendMessageOptions)
	for k, v := range b.queueMessages {
		queueMessages[k] = make([]SendMessageOptions, len(v))
		copy(queueMessages[k], v)
		delete(b.queueMessages, k)
	}
	b.mu.Unlock()
	
	// Publish exchange messages
	if len(messages) > 0 {
		_, err := b.client.Publish(ctx, messages...)
		if err != nil {
			log.Printf("Failed to flush exchange messages: %v", err)
			return err
		}
	}
	
	// Send queue messages
	for queueName, msgs := range queueMessages {
		if len(msgs) > 0 {
			_, err := b.client.Send(ctx, queueName, msgs...)
			if err != nil {
				log.Printf("Failed to flush queue messages to %s: %v", queueName, err)
				return err
			}
		}
	}
	
	return nil
}

func (b *Batcher) run() {
	defer b.client.wg.Done()
	
	for {
		select {
		case <-b.done:
			return
		case <-b.ticker.C:
			_ = b.Flush(context.Background())
		}
	}
}
