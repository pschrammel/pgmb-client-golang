package pgmb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// init loads environment variables from .env file if it exists
func init() {
	loadDotEnv()
}

// loadDotEnv loads environment variables from .env file using godotenv
func loadDotEnv() {
	// Try different possible paths for .env file
	possiblePaths := []string{
		".env",
		"../.env",
		"../../.env",
		"src/.env",
	}

	for _, path := range possiblePaths {
		if err := godotenv.Load(path); err == nil {
			// Successfully loaded .env file
			return
		}
	}

	// No .env file found or could be loaded, which is fine
	// Tests can still run with system environment variables
}

type TestConfig struct {
	AdminDSN string // Admin DSN for creating databases
	TestDSN  string // Test database DSN
	DBName   string // Name of the test database
}

// getTestConfig returns test configuration from environment variables
func getTestConfig() TestConfig {
	// Check for complete test DSN first
	if testDSN := os.Getenv("TEST_DSN"); testDSN != "" {
		return TestConfig{
			TestDSN: testDSN,
		}
	}

	// Build from individual components
	host := getEnvOrDefault("DB_HOST", "localhost")
	port := getEnvOrDefault("DB_PORT", "5432")
	user := getEnvOrDefault("DB_USER", "postgres")
	password := getEnvOrDefault("DB_PASSWORD", "password")
	adminDB := getEnvOrDefault("DB_ADMIN_DATABASE", "postgres")
	testDB := getEnvOrDefault("DB_TEST_DATABASE", "pgmb_test")
	sslMode := getEnvOrDefault("DB_SSLMODE", "disable")

	adminDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		user, password, host, port, adminDB, sslMode)

	testDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		user, password, host, port, testDB, sslMode)

	return TestConfig{
		AdminDSN: adminDSN,
		TestDSN:  testDSN,
		DBName:   testDB,
	}
}

// getEnvOrDefault returns environment variable value or default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// setupTestDB creates the test database and runs the PGMB SQL setup
func setupTestDB(t *testing.T) string {
	config := getTestConfig()

	// If we have a complete TEST_DSN, assume database is already set up
	if config.AdminDSN == "" {
		if config.TestDSN == "" || config.TestDSN == "skip" {
			t.Skip("Skipping test: TEST_DSN environment variable not set or set to 'skip'")
		}
		return config.TestDSN
	}

	ctx := context.Background()

	// Connect to admin database to create test database
	adminDB, err := sql.Open("postgres", config.AdminDSN)
	if err != nil {
		t.Fatalf("Failed to connect to admin database: %v", err)
	}
	defer adminDB.Close()

	// Test admin connection
	if err := adminDB.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping admin database: %v", err)
	}

	// Drop test database if it exists (cleanup from previous runs)
	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", config.DBName)
	if _, err := adminDB.ExecContext(ctx, dropSQL); err != nil {
		t.Logf("Warning: Failed to drop existing test database: %v", err)
	}

	// Create test database
	createSQL := fmt.Sprintf("CREATE DATABASE %s", config.DBName)
	if _, err := adminDB.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Close admin connection
	adminDB.Close()

	// Connect to the new test database
	testDB, err := sql.Open("postgres", config.TestDSN)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}
	defer testDB.Close()

	// Test connection to new database
	if err := testDB.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping test database: %v", err)
	}

	// Read and execute PGMB SQL file
	sqlContent, err := readPGMBSQL()
	if err != nil {
		t.Fatalf("Failed to read PGMB SQL file: %v", err)
	}

	// Execute the PGMB setup SQL
	if _, err := testDB.ExecContext(ctx, sqlContent); err != nil {
		t.Fatalf("Failed to execute PGMB setup SQL: %v", err)
	}

	/*t.Cleanup(func() {
		// Cleanup: drop test database after test
		adminDB, err := sql.Open("postgres", config.AdminDSN)
		if err != nil {
			t.Logf("Failed to connect to admin database for cleanup: %v", err)
			return
		}
		defer adminDB.Close()

		dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", config.DBName)
		if _, err := adminDB.ExecContext(context.Background(), dropSQL); err != nil {
			t.Logf("Failed to drop test database during cleanup: %v", err)
		}
	})*/

	return config.TestDSN
}

// readPGMBSQL reads the PGMB SQL file from sql/pgmb.sql
func readPGMBSQL() (string, error) {
	// Try different possible paths for the SQL file
	possiblePaths := []string{
		"sql/pgmb.sql",
		"../sql/pgmb.sql",
		"../../sql/pgmb.sql",
	}

	var sqlContent []byte
	var err error

	for _, path := range possiblePaths {
		if _, statErr := os.Stat(path); statErr == nil {
			sqlContent, err = os.ReadFile(path)
			if err == nil {
				break
			}
		}
	}

	if err != nil {
		return "", fmt.Errorf("failed to read PGMB SQL file from any of %v: %w", possiblePaths, err)
	}

	if len(sqlContent) == 0 {
		return "", fmt.Errorf("PGMB SQL file is empty")
	}

	return string(sqlContent), nil
}

// skipIfNoDSN skips the test if no test DSN is configured
func skipIfNoDSN(t *testing.T) string {
	config := getTestConfig()
	if config.TestDSN == "" || config.TestDSN == "skip" {
		t.Skip("Skipping test: TEST_DSN environment variable not set or set to 'skip'")
	}
	return config.TestDSN
}

func TestNewClient(t *testing.T) {
	dsn := setupTestDB(t)
	ctx := context.Background()

	client, err := NewClient(ctx, ClientOptions{
		DSN: dsn,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	if client == nil {
		t.Fatal("Client should not be nil")
	}
}

func TestClientAssertQueue(t *testing.T) {
	dsn := setupTestDB(t)
	ctx := context.Background()

	client, err := NewClient(ctx, ClientOptions{
		DSN: dsn,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	queueName := "test_queue"
	err = client.AssertQueue(ctx, QueueOptions{
		Name: queueName,
	})
	if err != nil {
		t.Fatalf("Failed to assert queue: %v", err)
	}
}

/*
func TestClientSendAndReceive(t *testing.T) {
	dsn := setupTestDB(t)
	ctx := context.Background()

	client, err := NewClient(ctx, ClientOptions{
		DSN: dsn,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	queueName := "test_send_receive_queue"

	// Assert queue exists
	err = client.AssertQueue(ctx, QueueOptions{
		Name: queueName,
	})
	if err != nil {
		t.Fatalf("Failed to assert queue: %v", err)
	}

	// Send a message
	testMessage := []byte("Hello, Test!")
	messages, err := client.Send(ctx, queueName, SendMessageOptions{
		Message: testMessage,
		Headers: map[string]any{"test": "value"},
	})
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	if messages[0].ID == "" {
		t.Fatal("Message ID should not be empty")
	}

	// Test consuming the message
	received := make(chan bool, 1)
	var receivedMessage []byte

	consumerClient, err := NewClient(ctx, ClientOptions{
		DSN: dsn,
		Consumers: []ConsumerOptions{
			{
				Name:      queueName,
				BatchSize: 1,
				OnMessage: func(ctx context.Context, queueName string, messages []Message, ack AckFunc) error {
					if len(messages) > 0 {
						receivedMessage = messages[0].Message
						ack(true, messages[0].ID)
						received <- true
					}
					return nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create consumer client: %v", err)
	}
	defer consumerClient.Close()

	// Start listening
	go func() {
		if err := consumerClient.Listen(); err != nil {
			t.Errorf("Listen failed: %v", err)
		}
	}()

	// Wait for message or timeout
	select {
	case <-received:
		if string(receivedMessage) != string(testMessage) {
			t.Fatalf("Expected message %q, got %q", testMessage, receivedMessage)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestClientWithExchange(t *testing.T) {
	dsn := setupTestDB(t)
	ctx := context.Background()

	client, err := NewClient(ctx, ClientOptions{
		DSN: dsn,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	exchangeName := "test_exchange"
	queueName := "test_exchange_queue"

	// Assert exchange
	err = client.AssertExchange(ctx, ExchangeOptions{
		Name: exchangeName,
		Type: "fanout",
	})
	if err != nil {
		t.Fatalf("Failed to assert exchange: %v", err)
	}

	// Assert queue
	err = client.AssertQueue(ctx, QueueOptions{
		Name: queueName,
	})
	if err != nil {
		t.Fatalf("Failed to assert queue: %v", err)
	}

	// Bind queue to exchange
	err = client.BindQueue(ctx, BindingOptions{
		Queue:    queueName,
		Exchange: exchangeName,
	})
	if err != nil {
		t.Fatalf("Failed to bind queue to exchange: %v", err)
	}
}

func TestClientBulkOperations(t *testing.T) {
	dsn := setupTestDB(t)
	ctx := context.Background()

	client, err := NewClient(ctx, ClientOptions{
		DSN: dsn,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	queueName := "test_bulk_queue"

	// Assert queue exists
	err = client.AssertQueue(ctx, QueueOptions{
		Name: queueName,
	})
	if err != nil {
		t.Fatalf("Failed to assert queue: %v", err)
	}

	// Send multiple messages
	messageOptions := []SendMessageOptions{
		{Message: []byte("Message 1"), Headers: map[string]any{"id": 1}},
		{Message: []byte("Message 2"), Headers: map[string]any{"id": 2}},
		{Message: []byte("Message 3"), Headers: map[string]any{"id": 3}},
	}

	messages, err := client.SendBulk(ctx, queueName, messageOptions)
	if err != nil {
		t.Fatalf("Failed to send bulk messages: %v", err)
	}

	if len(messages) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(messages))
	}

	for _, msg := range messages {
		if msg.ID == "" {
			t.Fatal("Message ID should not be empty")
		}
	}
}

func TestClientScheduledMessages(t *testing.T) {
	dsn := setupTestDB(t)
	ctx := context.Background()

	client, err := NewClient(ctx, ClientOptions{
		DSN: dsn,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	queueName := "test_scheduled_queue"

	// Assert queue exists
	err = client.AssertQueue(ctx, QueueOptions{
		Name: queueName,
	})
	if err != nil {
		t.Fatalf("Failed to assert queue: %v", err)
	}

	// Schedule a message for 2 seconds in the future
	scheduledTime := time.Now().Add(2 * time.Second)
	messages, err := client.Send(ctx, queueName, SendMessageOptions{
		Message:     []byte("Scheduled message"),
		ScheduledAt: &scheduledTime,
	})
	if err != nil {
		t.Fatalf("Failed to send scheduled message: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}
}

// Test database creation and setup specifically
func TestDatabaseSetup(t *testing.T) {
	config := getTestConfig()
	if config.AdminDSN == "" {
		t.Skip("Skipping database setup test: AdminDSN not available")
	}

	ctx := context.Background()

	// Connect to admin database
	adminDB, err := sql.Open("postgres", config.AdminDSN)
	if err != nil {
		t.Fatalf("Failed to connect to admin database: %v", err)
	}
	defer adminDB.Close()

	// Test that we can connect
	if err := adminDB.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping admin database: %v", err)
	}

	// Test that we can read the SQL file
	sqlContent, err := readPGMBSQL()
	if err != nil {
		t.Fatalf("Failed to read PGMB SQL: %v", err)
	}

	if len(strings.TrimSpace(sqlContent)) == 0 {
		t.Fatal("PGMB SQL content is empty")
	}

	// Verify SQL content contains expected PGMB elements
	expectedElements := []string{"CREATE", "FUNCTION", "TRIGGER", "TABLE"}
	for _, element := range expectedElements {
		if !strings.Contains(strings.ToUpper(sqlContent), element) {
			t.Logf("Warning: Expected element '%s' not found in SQL content", element)
		}
	}
}

// Benchmark tests
func BenchmarkClientSend(b *testing.B) {
	config := getTestConfig()
	if config.TestDSN == "" || config.TestDSN == "skip" {
		b.Skip("Skipping benchmark: TEST_DSN environment variable not set or set to 'skip'")
	}

	ctx := context.Background()
	client, err := NewClient(ctx, ClientOptions{
		DSN: config.TestDSN,
	})
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	queueName := "bench_queue"
	err = client.AssertQueue(ctx, QueueOptions{
		Name: queueName,
	})
	if err != nil {
		b.Fatalf("Failed to assert queue: %v", err)
	}

	message := []byte("benchmark message")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Send(ctx, queueName, SendMessageOptions{
				Message: message,
			})
			if err != nil {
				b.Fatalf("Failed to send message: %v", err)
			}
		}
	})
}
*/
