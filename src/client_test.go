package pgmb

import (
	"context"
	"database/sql"
	"testing"

	//"time"

	_ "github.com/lib/pq"
)

const testDSN = "postgres://postgres:postgres@dev.conceptboard.com:30432/pgmb_test?sslmode=disable"

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("postgres", testDSN)
	if err != nil {
		t.Skip("PostgreSQL not available for testing:", err)
	}

	if err := db.Ping(); err != nil {
		t.Skip("PostgreSQL not available for testing:", err)
	}

	return db
}

func TestNewClient(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx := context.Background()

	client, err := NewClient(ctx, ClientOptions{
		DB: db,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	if client.db != db {
		t.Error("Client DB not set correctly")
	}
}

func TestNewClientWithDSN(t *testing.T) {
	ctx := context.Background()

	client, err := NewClient(ctx, ClientOptions{
		DSN: testDSN,
	})
	if err != nil {
		t.Fatalf("Failed to create client with DSN: %v", err)
	}
	defer client.Close()

	if client.db == nil {
		t.Error("Client DB not created from DSN")
	}
}

/* func TestBatcher(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx := context.Background()
	client, err := NewClient(ctx, ClientOptions{
		DB: db,
		Batcher: &BatcherOptions{
			MaxBatchSize: 5,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	batcher := client.GetDefaultBatcher()
	if batcher == nil {
		t.Fatal("Batcher not created")
	}

	// Test enqueuing to exchange
	for i := 0; i < 3; i++ {
		batcher.Enqueue(PublishMessageOptions{
			Exchange: "test_exchange",
			Message:  []byte("test message"),
		})
	}

	// Test enqueuing to queue
	for i := 0; i < 2; i++ {
		batcher.EnqueueToQueue("test_queue", SendMessageOptions{
			Message: []byte("direct message"),
		})
	}

	// Flush manually
	err = batcher.Flush(ctx)
	if err != nil {
		t.Errorf("Failed to flush batcher: %v", err)
	}
}
*/
