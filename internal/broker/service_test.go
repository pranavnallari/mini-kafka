package broker

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/prananallari/mini-kafka/internal/storage"
)

func setupTestService(t *testing.T) *Service {
	t.Helper()

	postgresURL := os.Getenv("TEST_POSTGRES_URL")
	redisAddr := os.Getenv("TEST_REDIS_ADDR")

	if postgresURL == "" || redisAddr == "" {
		t.Skip("skipping integration test: TEST_POSTGRES_URL or TEST_REDIS_ADDR not set")
	}

	db, err := storage.NewPostgres(postgresURL)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	if err := storage.InitSchema(db); err != nil {
		t.Fatalf("failed to init schema: %v", err)
	}

	rdb := storage.NewRedis(redisAddr, "")

	t.Cleanup(func() {
		cleanupDB(t, db)
		db.Close()
		rdb.Close()
	})

	return NewService(db, rdb)
}

func cleanupDB(t *testing.T, db *sql.DB) {
	t.Helper()
	tables := []string{"retry_messages", "consumer_offsets", "partitions", "consumer_groups", "topics"}
	for _, table := range tables {
		if _, err := db.Exec("DELETE FROM " + table); err != nil {
			t.Logf("failed to clean table %s: %v", table, err)
		}
	}
}

func TestCreateTopic(t *testing.T) {
	s := setupTestService(t)
	ctx := context.Background()

	err := s.CreateTopic(ctx, "test-topic", 3)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// creating the same topic again should fail
	err = s.CreateTopic(ctx, "test-topic", 3)
	if err == nil {
		t.Error("expected error for duplicate topic, got nil")
	}
}

func TestPublishAndConsume(t *testing.T) {
	s := setupTestService(t)
	ctx := context.Background()

	if err := s.CreateTopic(ctx, "payments", 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if err := s.CreateGroup(ctx, "billing"); err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	if err := s.Publish(ctx, "payments", "order-1", []byte("payload-1")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	if err := s.Publish(ctx, "payments", "order-1", []byte("payload-2")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	messages, err := s.Consume(ctx, "payments", "billing", 0, 10)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	if string(messages[0]) != "payload-1" {
		t.Errorf("expected payload-1, got %s", messages[0])
	}
}

func TestConsume_OffsetAdvances(t *testing.T) {
	s := setupTestService(t)
	ctx := context.Background()

	if err := s.CreateTopic(ctx, "orders", 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	for i := 0; i < 5; i++ {
		if err := s.Publish(ctx, "orders", "key", []byte("msg")); err != nil {
			t.Fatalf("failed to publish: %v", err)
		}
	}

	first, err := s.Consume(ctx, "orders", "shipping", 0, 3)
	if err != nil {
		t.Fatalf("first consume failed: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(first))
	}

	second, err := s.Consume(ctx, "orders", "shipping", 0, 3)
	if err != nil {
		t.Fatalf("second consume failed: %v", err)
	}
	if len(second) != 2 {
		t.Fatalf("expected 2 remaining messages, got %d", len(second))
	}
}

func TestConsume_TwoGroupsAreIndependent(t *testing.T) {
	s := setupTestService(t)
	ctx := context.Background()

	if err := s.CreateTopic(ctx, "events", 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if err := s.Publish(ctx, "events", "key", []byte("event-1")); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	groupA, err := s.Consume(ctx, "events", "group-a", 0, 10)
	if err != nil {
		t.Fatalf("group-a consume failed: %v", err)
	}

	groupB, err := s.Consume(ctx, "events", "group-b", 0, 10)
	if err != nil {
		t.Fatalf("group-b consume failed: %v", err)
	}

	if len(groupA) != 1 || len(groupB) != 1 {
		t.Errorf("expected both groups to independently read 1 message")
	}
}

func TestRetry(t *testing.T) {
	s := setupTestService(t)
	ctx := context.Background()

	err := s.Retry(ctx, "payments", "billing", 0, 0, "failed-payload")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// duplicate retry should be silently ignored
	err = s.Retry(ctx, "payments", "billing", 0, 0, "failed-payload")
	if err != nil {
		t.Fatalf("expected duplicate retry to be ignored, got: %v", err)
	}
}
