package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandlePublish(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")

	reqBody := bytes.NewBufferString(`{
		"messages": [
			{"data": "dGVzdDE=", "attributes": {"key": "value"}},
			{"data": "dGVzdDI="}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp PublishResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.MessageIDs) != 2 {
		t.Errorf("Expected 2 message IDs, got %d", len(resp.MessageIDs))
	}
}

func TestHandlePublish_TopicNotFound(t *testing.T) {
	server := NewServer()

	reqBody := bytes.NewBufferString(`{
		"messages": [
			{"data": "dGVzdA=="}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/nonexistent:publish", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestHandlePublish_InvalidJSON(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")

	reqBody := bytes.NewBufferString(`invalid json`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestHandlePull(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish messages
	messages := []PubSubMessage{
		{Data: "dGVzdDE="},
		{Data: "dGVzdDI="},
	}
	server.storage.Publish("projects/test/topics/topic1", messages)

	// Wait for deadline
	time.Sleep(100 * time.Millisecond)

	// Pull messages
	reqBody := bytes.NewBufferString(`{"maxMessages": 10}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp PullResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.ReceivedMessages) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(resp.ReceivedMessages))
	}
}

func TestHandlePull_EmptyQueue(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Pull without messages
	reqBody := bytes.NewBufferString(`{"maxMessages": 10}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp PullResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.ReceivedMessages) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(resp.ReceivedMessages))
	}
}

func TestHandlePull_SubscriptionNotFound(t *testing.T) {
	server := NewServer()

	reqBody := bytes.NewBufferString(`{"maxMessages": 10}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/nonexistent:pull", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestHandleAcknowledge(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish and pull messages
	messages := []PubSubMessage{{Data: "dGVzdA=="}}
	server.storage.Publish("projects/test/topics/topic1", messages)
	time.Sleep(100 * time.Millisecond)

	pulled, _ := server.storage.Pull("projects/test/subscriptions/sub1", 10)

	// Acknowledge
	ackReq := AcknowledgeRequest{AckIDs: []string{pulled[0].AckID}}
	reqBodyBytes, _ := json.Marshal(ackReq)
	reqBody := bytes.NewBuffer(reqBodyBytes)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:acknowledge", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestHandleAcknowledge_SubscriptionNotFound(t *testing.T) {
	server := NewServer()

	reqBody := bytes.NewBufferString(`{"ackIds": ["test-ack-id"]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/nonexistent:acknowledge", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestHandleAcknowledge_InvalidJSON(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	reqBody := bytes.NewBufferString(`invalid json`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:acknowledge", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestHandleModifyAckDeadline(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish and pull messages
	messages := []PubSubMessage{{Data: "dGVzdA=="}}
	server.storage.Publish("projects/test/topics/topic1", messages)
	time.Sleep(100 * time.Millisecond)

	pulled, _ := server.storage.Pull("projects/test/subscriptions/sub1", 10)

	// Modify ack deadline
	modifyReq := ModifyAckDeadlineRequest{
		AckIDs:             []string{pulled[0].AckID},
		AckDeadlineSeconds: 30,
	}
	reqBodyBytes, _ := json.Marshal(modifyReq)
	reqBody := bytes.NewBuffer(reqBodyBytes)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:modifyAckDeadline", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestHandleModifyAckDeadline_SubscriptionNotFound(t *testing.T) {
	server := NewServer()

	reqBody := bytes.NewBufferString(`{"ackIds": ["test-ack-id"], "ackDeadlineSeconds": 30}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/nonexistent:modifyAckDeadline", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestHandleModifyAckDeadline_InvalidJSON(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	reqBody := bytes.NewBufferString(`invalid json`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:modifyAckDeadline", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestHandleModifyAckDeadline_InvalidAckID(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Try to modify with invalid ack ID
	modifyReq := ModifyAckDeadlineRequest{
		AckIDs:             []string{"invalid-ack-id"},
		AckDeadlineSeconds: 30,
	}
	reqBodyBytes, _ := json.Marshal(modifyReq)
	reqBody := bytes.NewBuffer(reqBodyBytes)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:modifyAckDeadline", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}
