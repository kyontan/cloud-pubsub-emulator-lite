package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestUseCase_MultipleSubscriptions tests fan-out pattern (one message to multiple subscribers)
func TestUseCase_MultipleSubscriptions(t *testing.T) {
	server := NewServer()

	// Step 1: Create a topic
	t.Log("Creating topic...")
	req := httptest.NewRequest(http.MethodPut, "/v1/projects/test/topics/topic1", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	// Step 2: Create multiple subscriptions
	t.Log("Creating subscription 1...")
	reqBody := bytes.NewBufferString(`{"topic": "projects/test/topics/topic1"}`)
	req = httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub1", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	t.Log("Creating subscription 2...")
	reqBody = bytes.NewBufferString(`{"topic": "projects/test/topics/topic1"}`)
	req = httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub2", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	t.Log("Creating subscription 3...")
	reqBody = bytes.NewBufferString(`{"topic": "projects/test/topics/topic1"}`)
	req = httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub3", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	// Step 3: Publish a message
	t.Log("Publishing message...")
	reqBody = bytes.NewBufferString(`{
		"messages": [
			{"data": "YnJvYWRjYXN0", "attributes": {"type": "broadcast"}}
		]
	}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	// Wait for deadline
	time.Sleep(100 * time.Millisecond)

	// Step 4: All subscriptions should receive the message
	t.Log("Pulling from subscription 1...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp1 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp1)
	if len(pullResp1.ReceivedMessages) != 1 {
		t.Errorf("Expected 1 message in sub1, got %d", len(pullResp1.ReceivedMessages))
	}

	t.Log("Pulling from subscription 2...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub2:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2)
	if len(pullResp2.ReceivedMessages) != 1 {
		t.Errorf("Expected 1 message in sub2, got %d", len(pullResp2.ReceivedMessages))
	}

	t.Log("Pulling from subscription 3...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub3:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp3 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp3)
	if len(pullResp3.ReceivedMessages) != 1 {
		t.Errorf("Expected 1 message in sub3, got %d", len(pullResp3.ReceivedMessages))
	}

	// Step 5: Acknowledge in sub1 only
	t.Log("Acknowledging in subscription 1...")
	ackReq := AcknowledgeRequest{AckIDs: []string{pullResp1.ReceivedMessages[0].AckID}}
	reqBodyBytes, _ := json.Marshal(ackReq)
	reqBody = bytes.NewBuffer(reqBodyBytes)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:acknowledge", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	// Step 6: Verify sub1 has no messages, but sub2 and sub3 still have them
	time.Sleep(100 * time.Millisecond)

	t.Log("Verifying subscription 1 is empty...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp1Again PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp1Again)
	if len(pullResp1Again.ReceivedMessages) != 0 {
		t.Errorf("Expected 0 messages in sub1 after ack, got %d", len(pullResp1Again.ReceivedMessages))
	}

	t.Log("Verifying subscription 2 still has message...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub2:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2Again PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2Again)
	if len(pullResp2Again.ReceivedMessages) != 1 {
		t.Errorf("Expected 1 message in sub2, got %d", len(pullResp2Again.ReceivedMessages))
	}

	t.Log("Multiple subscriptions test completed successfully")
}

// TestUseCase_PartialAcknowledge tests acknowledging some messages but not others
func TestUseCase_PartialAcknowledge(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish 3 messages
	t.Log("Publishing 3 messages...")
	reqBody := bytes.NewBufferString(`{
		"messages": [
			{"data": "bXNnMQ=="},
			{"data": "bXNnMg=="},
			{"data": "bXNnMw=="}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	time.Sleep(100 * time.Millisecond)

	// Pull all messages
	t.Log("Pulling all messages...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 10}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(pullResp.ReceivedMessages))
	}

	// Acknowledge only first and third messages
	t.Log("Acknowledging messages 1 and 3...")
	ackReq := AcknowledgeRequest{
		AckIDs: []string{
			pullResp.ReceivedMessages[0].AckID,
			pullResp.ReceivedMessages[2].AckID,
		},
	}
	reqBodyBytes, _ := json.Marshal(ackReq)
	reqBody = bytes.NewBuffer(reqBodyBytes)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:acknowledge", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	time.Sleep(100 * time.Millisecond)

	// Pull again - should only get the second message
	t.Log("Pulling remaining messages...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 10}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2)

	if len(pullResp2.ReceivedMessages) != 1 {
		t.Errorf("Expected 1 message remaining, got %d", len(pullResp2.ReceivedMessages))
	}

	if pullResp2.ReceivedMessages[0].Message.Data != "bXNnMg==" {
		t.Errorf("Expected second message to remain, got %s", pullResp2.ReceivedMessages[0].Message.Data)
	}

	t.Log("Partial acknowledge test completed successfully")
}
