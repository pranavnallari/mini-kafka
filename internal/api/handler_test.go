package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockService struct {
	createTopicFn func(ctx context.Context, name string, partitionCount int) error
	createGroupFn func(ctx context.Context, groupName string) error
	publishFn     func(ctx context.Context, topicName, key string, payload []byte) error
	consumeFn     func(ctx context.Context, topicName, groupName string, partitionIndex, limit int) ([][]byte, error)
	retryFn       func(ctx context.Context, topicName, groupName string, offset, partitionIndex int, payload string) error
}

func (m *mockService) CreateTopic(ctx context.Context, name string, partitionCount int) error {
	return m.createTopicFn(ctx, name, partitionCount)
}

func (m *mockService) CreateGroup(ctx context.Context, groupName string) error {
	return m.createGroupFn(ctx, groupName)
}

func (m *mockService) Publish(ctx context.Context, topicName, key string, payload []byte) error {
	return m.publishFn(ctx, topicName, key, payload)
}

func (m *mockService) Consume(ctx context.Context, topicName, groupName string, partitionIndex, limit int) ([][]byte, error) {
	return m.consumeFn(ctx, topicName, groupName, partitionIndex, limit)
}

func (m *mockService) Retry(ctx context.Context, topicName, groupName string, offset, partitionIndex int, payload string) error {
	return m.retryFn(ctx, topicName, groupName, offset, partitionIndex, payload)
}

func TestCreateTopic_Success(t *testing.T) {
	mock := &mockService{
		createTopicFn: func(ctx context.Context, name string, partitionCount int) error {
			return nil
		},
	}

	h := NewHandler(mock)
	body, _ := json.Marshal(map[string]any{"name": "payments", "partition_count": 3})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateTopic(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", w.Code)
	}
}

func TestCreateTopic_InvalidBody(t *testing.T) {
	h := NewHandler(&mockService{})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader([]byte("invalid json")))
	w := httptest.NewRecorder()

	h.CreateTopic(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestCreateTopic_MissingFields(t *testing.T) {
	h := NewHandler(&mockService{})
	body, _ := json.Marshal(map[string]any{"name": ""})
	req := httptest.NewRequest(http.MethodPost, "/topics", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.CreateTopic(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestPublish_Success(t *testing.T) {
	mock := &mockService{
		publishFn: func(ctx context.Context, topicName, key string, payload []byte) error {
			return nil
		},
	}

	h := NewHandler(mock)
	body, _ := json.Marshal(map[string]any{"key": "order-1", "payload": "hello"})
	req := httptest.NewRequest(http.MethodPost, "/topics/payments/messages", bytes.NewReader(body))
	req.SetPathValue("name", "payments")
	w := httptest.NewRecorder()

	h.Publish(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", w.Code)
	}
}

func TestConsume_Success(t *testing.T) {
	mock := &mockService{
		consumeFn: func(ctx context.Context, topicName, groupName string, partitionIndex, limit int) ([][]byte, error) {
			return [][]byte{[]byte("msg-1"), []byte("msg-2")}, nil
		},
	}

	h := NewHandler(mock)
	req := httptest.NewRequest(http.MethodGet, "/topics/payments/messages?group=billing&partition=0&limit=10", nil)
	req.SetPathValue("name", "payments")
	w := httptest.NewRecorder()

	h.Consume(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp []string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp) != 2 {
		t.Errorf("expected 2 messages, got %d", len(resp))
	}
}

func TestConsume_MissingParams(t *testing.T) {
	h := NewHandler(&mockService{})
	req := httptest.NewRequest(http.MethodGet, "/topics/payments/messages", nil)
	req.SetPathValue("name", "payments")
	w := httptest.NewRecorder()

	h.Consume(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestRetryMessages_Success(t *testing.T) {
	mock := &mockService{
		retryFn: func(ctx context.Context, topicName, groupName string, offset, partitionIndex int, payload string) error {
			return nil
		},
	}

	h := NewHandler(mock)
	body, _ := json.Marshal(map[string]any{
		"group_name":      "billing",
		"partition_index": 0,
		"offset":          0,
		"payload":         "failed-payload",
	})
	req := httptest.NewRequest(http.MethodPost, "/topics/payments/messages/retry", bytes.NewReader(body))
	req.SetPathValue("name", "payments")
	w := httptest.NewRecorder()

	h.RetryMessages(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestRetryMessages_MissingFields(t *testing.T) {
	h := NewHandler(&mockService{})
	body, _ := json.Marshal(map[string]any{"group_name": ""})
	req := httptest.NewRequest(http.MethodPost, "/topics/payments/messages/retry", bytes.NewReader(body))
	req.SetPathValue("name", "payments")
	w := httptest.NewRecorder()

	h.RetryMessages(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
