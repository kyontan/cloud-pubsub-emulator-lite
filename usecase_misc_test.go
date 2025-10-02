package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestUseCase_MaxMessages tests pulling with maxMessages limit
func TestUseCase_MaxMessages(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish 10 messages
	t.Log("Publishing 10 messages...")
	messages := make([]PubSubMessage, 10)
	for i := 0; i < 10; i++ {
		messages[i] = PubSubMessage{Data: "dGVzdA=="}
	}
	publishReq := PublishRequest{Messages: messages}
	reqBodyBytes, _ := json.Marshal(publishReq)
	reqBody := bytes.NewBuffer(reqBodyBytes)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	time.Sleep(100 * time.Millisecond)

	// Pull with maxMessages = 3
	t.Log("Pulling with maxMessages = 3...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 3}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(pullResp.ReceivedMessages))
	}

	// Acknowledge the pulled messages
	ackIDs := make([]string, len(pullResp.ReceivedMessages))
	for i, msg := range pullResp.ReceivedMessages {
		ackIDs[i] = msg.AckID
	}
	ackReq := AcknowledgeRequest{AckIDs: ackIDs}
	reqBodyBytes, _ = json.Marshal(ackReq)
	reqBody = bytes.NewBuffer(reqBodyBytes)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:acknowledge", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	time.Sleep(100 * time.Millisecond)

	// Pull again - should get 7 remaining messages with maxMessages = 5
	t.Log("Pulling with maxMessages = 5...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 5}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2)

	if len(pullResp2.ReceivedMessages) != 5 {
		t.Errorf("Expected 5 messages, got %d", len(pullResp2.ReceivedMessages))
	}

	t.Log("MaxMessages test completed successfully")
}

// TestUseCase_HealthCheck tests the health check endpoint
func TestUseCase_HealthCheck(t *testing.T) {
	server := NewServer()

	t.Log("Testing health check endpoint...")
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.handleHealthCheck)
	mux.Handle("/", server)

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	body := w.Body.String()
	if body != "OK" {
		t.Errorf("Expected body 'OK', got '%s'", body)
	}

	t.Log("Health check test completed successfully")
}

// TestUseCase_MessageAttributes tests publishing and receiving messages with attributes
func TestUseCase_MessageAttributes(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish message with attributes
	t.Log("Publishing message with attributes...")
	reqBody := bytes.NewBufferString(`{
		"messages": [
			{
				"data": "dGVzdCBkYXRh",
				"attributes": {
					"key1": "value1",
					"key2": "value2",
					"priority": "high"
				}
			}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	time.Sleep(100 * time.Millisecond)

	// Pull and verify attributes
	t.Log("Pulling and verifying attributes...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(pullResp.ReceivedMessages))
	}

	msg := pullResp.ReceivedMessages[0].Message
	if msg.Attributes["key1"] != "value1" {
		t.Errorf("Expected attribute key1='value1', got '%s'", msg.Attributes["key1"])
	}
	if msg.Attributes["key2"] != "value2" {
		t.Errorf("Expected attribute key2='value2', got '%s'", msg.Attributes["key2"])
	}
	if msg.Attributes["priority"] != "high" {
		t.Errorf("Expected attribute priority='high', got '%s'", msg.Attributes["priority"])
	}

	t.Log("Message attributes test completed successfully")
}

// TestUseCase_EmptyPull tests pulling when no messages are available
func TestUseCase_EmptyPull(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Pull without publishing any messages
	t.Log("Pulling from empty subscription...")
	reqBody := bytes.NewBufferString(`{"maxMessages": 10}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(pullResp.ReceivedMessages))
	}

	t.Log("Empty pull test completed successfully")
}

// TestUseCase_MessageIDGeneration tests that unique message IDs are generated
func TestUseCase_MessageIDGeneration(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")

	// Publish multiple messages
	t.Log("Publishing messages and checking IDs...")
	reqBody := bytes.NewBufferString(`{
		"messages": [
			{"data": "bWVzc2FnZTE="},
			{"data": "bWVzc2FnZTI="},
			{"data": "bWVzc2FnZTM="}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var publishResp PublishResponse
	json.NewDecoder(w.Body).Decode(&publishResp)

	if len(publishResp.MessageIDs) != 3 {
		t.Fatalf("Expected 3 message IDs, got %d", len(publishResp.MessageIDs))
	}

	// Verify all message IDs are unique
	idMap := make(map[string]bool)
	for _, id := range publishResp.MessageIDs {
		if id == "" {
			t.Error("Message ID should not be empty")
		}
		if idMap[id] {
			t.Errorf("Duplicate message ID found: %s", id)
		}
		idMap[id] = true
	}

	t.Log("Message ID generation test completed successfully")
}

// TestUseCase_PullWithoutMaxMessages tests default behavior when maxMessages is not specified
func TestUseCase_PullWithoutMaxMessages(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish messages
	reqBody := bytes.NewBufferString(`{
		"messages": [
			{"data": "bWVzc2FnZTE="},
			{"data": "bWVzc2FnZTI="}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	time.Sleep(100 * time.Millisecond)

	// Pull without specifying maxMessages (should default to 1)
	t.Log("Pulling without maxMessages...")
	reqBody = bytes.NewBufferString(`{}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 1 {
		t.Errorf("Expected 1 message (default), got %d", len(pullResp.ReceivedMessages))
	}

	t.Log("Pull without maxMessages test completed successfully")
}
