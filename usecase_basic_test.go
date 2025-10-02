package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestUseCase_BasicPubSub tests the basic publish-subscribe workflow
func TestUseCase_BasicPubSub(t *testing.T) {
	server := NewServer()

	// Step 1: Create a topic
	t.Log("Creating topic...")
	req := httptest.NewRequest(http.MethodPut, "/v1/projects/myproject/topics/mytopic", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to create topic: %d", w.Code)
	}

	// Step 2: Create a subscription
	t.Log("Creating subscription...")
	reqBody := bytes.NewBufferString(`{"topic": "projects/myproject/topics/mytopic"}`)
	req = httptest.NewRequest(http.MethodPut, "/v1/projects/myproject/subscriptions/mysub", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to create subscription: %d", w.Code)
	}

	// Step 3: Publish a message
	t.Log("Publishing message...")
	reqBody = bytes.NewBufferString(`{
		"messages": [
			{"data": "SGVsbG8gV29ybGQ=", "attributes": {"sender": "test"}}
		]
	}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/myproject/topics/mytopic:publish", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to publish message: %d", w.Code)
	}

	// Step 4: Wait for ack deadline
	t.Log("Waiting for ack deadline...")
	time.Sleep(100 * time.Millisecond)

	// Step 5: Pull the message
	t.Log("Pulling message...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/myproject/subscriptions/mysub:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to pull message: %d", w.Code)
	}

	var pullResp PullResponse
	if err := json.NewDecoder(w.Body).Decode(&pullResp); err != nil {
		t.Fatalf("Failed to decode pull response: %v", err)
	}

	if len(pullResp.ReceivedMessages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(pullResp.ReceivedMessages))
	}

	msg := pullResp.ReceivedMessages[0]
	if msg.Message.Data != "SGVsbG8gV29ybGQ=" {
		t.Errorf("Expected data 'SGVsbG8gV29ybGQ=', got %s", msg.Message.Data)
	}
	if msg.Message.Attributes["sender"] != "test" {
		t.Errorf("Expected sender 'test', got %s", msg.Message.Attributes["sender"])
	}

	// Step 6: Acknowledge the message
	t.Log("Acknowledging message...")
	ackReq := AcknowledgeRequest{AckIDs: []string{msg.AckID}}
	reqBodyBytes, _ := json.Marshal(ackReq)
	reqBody = bytes.NewBuffer(reqBodyBytes)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/myproject/subscriptions/mysub:acknowledge", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to acknowledge message: %d", w.Code)
	}

	// Step 7: Verify message is gone
	t.Log("Verifying message is acknowledged...")
	time.Sleep(100 * time.Millisecond)
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/myproject/subscriptions/mysub:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2)
	if len(pullResp2.ReceivedMessages) != 0 {
		t.Errorf("Expected 0 messages after ack, got %d", len(pullResp2.ReceivedMessages))
	}

	t.Log("Basic pub/sub test completed successfully")
}

// TestUseCase_MultipleMessages tests publishing and receiving multiple messages
func TestUseCase_MultipleMessages(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish multiple messages
	t.Log("Publishing 5 messages...")
	reqBody := bytes.NewBufferString(`{
		"messages": [
			{"data": "bWVzc2FnZTE="},
			{"data": "bWVzc2FnZTI="},
			{"data": "bWVzc2FnZTM="},
			{"data": "bWVzc2FnZTQ="},
			{"data": "bWVzc2FnZTU="}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to publish messages: %d", w.Code)
	}

	// Wait for deadline
	time.Sleep(100 * time.Millisecond)

	// Pull all messages
	t.Log("Pulling all messages...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 10}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 5 {
		t.Errorf("Expected 5 messages, got %d", len(pullResp.ReceivedMessages))
	}

	t.Log("Multiple messages test completed successfully")
}
