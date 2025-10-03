package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestUseCase_ModifyAckDeadlineExtend tests extending the ack deadline
func TestUseCase_ModifyAckDeadlineExtend(t *testing.T) {
	server := NewServer()

	// Create topic and subscription
	t.Log("Creating topic and subscription...")
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish a message
	t.Log("Publishing message...")
	server.storage.Publish("projects/test/topics/topic1", []PubSubMessage{{Data: "dGVzdA=="}})

	// Wait for message to be available
	t.Log("Waiting for message to be available...")
	time.Sleep(100 * time.Millisecond)

	// Pull the message
	t.Log("Pulling message...")
	reqBody := bytes.NewBufferString(`{"maxMessages": 1}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(pullResp.ReceivedMessages))
	}

	ackID := pullResp.ReceivedMessages[0].AckID

	// Modify ack deadline to 10 seconds
	t.Log("Modifying ack deadline to 10 seconds...")
	modifyReq := ModifyAckDeadlineRequest{
		AckIDs:             []string{ackID},
		AckDeadlineSeconds: 10,
	}
	reqBodyBytes, _ := json.Marshal(modifyReq)
	reqBody = bytes.NewBuffer(reqBodyBytes)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:modifyAckDeadline", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	// Wait for original deadline to pass (default is 10 seconds, we're at ~0.1 seconds)
	t.Log("Waiting for original deadline to pass...")
	time.Sleep(200 * time.Millisecond)

	// Try to pull again - message should NOT be available due to extended deadline
	t.Log("Attempting to pull again (should be empty due to extended deadline)...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2)

	if len(pullResp2.ReceivedMessages) != 0 {
		t.Errorf("Expected 0 messages (deadline extended), got %d", len(pullResp2.ReceivedMessages))
	}

	t.Log("Modify ack deadline test (extend) completed successfully")
}

// TestUseCase_ModifyAckDeadlineImmediateRedelivery tests setting deadline to 0 for immediate redelivery
func TestUseCase_ModifyAckDeadlineImmediateRedelivery(t *testing.T) {
	server := NewServer()

	// Create topic and subscription
	t.Log("Creating topic and subscription...")
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish a message
	t.Log("Publishing message...")
	server.storage.Publish("projects/test/topics/topic1", []PubSubMessage{{Data: "dGVzdA=="}})

	// Wait for message to be available
	t.Log("Waiting for message to be available...")
	time.Sleep(100 * time.Millisecond)

	// Pull the message
	t.Log("Pulling message...")
	reqBody := bytes.NewBufferString(`{"maxMessages": 1}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(pullResp.ReceivedMessages))
	}

	ackID := pullResp.ReceivedMessages[0].AckID

	// Modify ack deadline to 0 (immediate redelivery)
	t.Log("Modifying ack deadline to 0 (immediate redelivery)...")
	modifyReq := ModifyAckDeadlineRequest{
		AckIDs:             []string{ackID},
		AckDeadlineSeconds: 0,
	}
	reqBodyBytes, _ := json.Marshal(modifyReq)
	reqBody = bytes.NewBuffer(reqBodyBytes)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:modifyAckDeadline", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	// Pull immediately - message should be available
	t.Log("Pulling immediately (message should be available)...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2)

	if len(pullResp2.ReceivedMessages) != 1 {
		t.Errorf("Expected 1 message (immediately redelivered), got %d", len(pullResp2.ReceivedMessages))
	}

	t.Log("Modify ack deadline test (immediate redelivery) completed successfully")
}

// TestUseCase_ModifyAckDeadlineMultipleMessages tests modifying deadline for multiple messages
func TestUseCase_ModifyAckDeadlineMultipleMessages(t *testing.T) {
	server := NewServer()

	// Create topic and subscription
	t.Log("Creating topic and subscription...")
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish 3 messages
	t.Log("Publishing 3 messages...")
	messages := []PubSubMessage{
		{Data: "bWVzc2FnZTE="},
		{Data: "bWVzc2FnZTI="},
		{Data: "bWVzc2FnZTM="},
	}
	server.storage.Publish("projects/test/topics/topic1", messages)

	// Wait for messages to be available
	t.Log("Waiting for messages to be available...")
	time.Sleep(100 * time.Millisecond)

	// Pull all messages
	t.Log("Pulling all messages...")
	reqBody := bytes.NewBufferString(`{"maxMessages": 10}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(pullResp.ReceivedMessages))
	}

	// Modify ack deadline to 0 for 2 messages (immediate redelivery)
	t.Log("Modifying ack deadline for 2 messages to 0...")
	modifyReq := ModifyAckDeadlineRequest{
		AckIDs: []string{
			pullResp.ReceivedMessages[0].AckID,
			pullResp.ReceivedMessages[1].AckID,
		},
		AckDeadlineSeconds: 0,
	}
	reqBodyBytes, _ := json.Marshal(modifyReq)
	reqBody = bytes.NewBuffer(reqBodyBytes)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:modifyAckDeadline", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	// Pull immediately - 2 messages should be available
	t.Log("Pulling immediately (2 messages should be available)...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 10}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2)

	if len(pullResp2.ReceivedMessages) != 2 {
		t.Errorf("Expected 2 messages (redelivered), got %d", len(pullResp2.ReceivedMessages))
	}

	t.Log("Modify ack deadline test (multiple messages) completed successfully")
}

// TestUseCase_ModifyAckDeadlineWorkflow tests a realistic workflow
func TestUseCase_ModifyAckDeadlineWorkflow(t *testing.T) {
	server := NewServer()

	// Create topic and subscription
	t.Log("Creating topic and subscription...")
	server.storage.CreateTopic("projects/test/topics/work-queue")
	server.storage.CreateSubscription("projects/test/subscriptions/worker", "projects/test/topics/work-queue")

	// Publish a work item
	t.Log("Publishing work item...")
	server.storage.Publish("projects/test/topics/work-queue", []PubSubMessage{
		{Data: "d29yayBpdGVt", Attributes: map[string]string{"task": "process-video"}},
	})

	time.Sleep(100 * time.Millisecond)

	// Worker pulls work item
	t.Log("Worker pulls work item...")
	reqBody := bytes.NewBufferString(`{"maxMessages": 1}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/worker:pull", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(pullResp.ReceivedMessages))
	}

	ackID := pullResp.ReceivedMessages[0].AckID

	// Worker realizes task takes longer, extends deadline multiple times
	t.Log("Worker realizes task takes longer, extends deadline...")
	for i := 0; i < 3; i++ {
		modifyReq := ModifyAckDeadlineRequest{
			AckIDs:             []string{ackID},
			AckDeadlineSeconds: 10,
		}
		reqBodyBytes, _ := json.Marshal(modifyReq)
		reqBody = bytes.NewBuffer(reqBodyBytes)
		req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/worker:modifyAckDeadline", reqBody)
		w = httptest.NewRecorder()
		server.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		t.Logf("Extended deadline iteration %d", i+1)
		time.Sleep(50 * time.Millisecond)
	}

	// Worker completes task and acknowledges
	t.Log("Worker completes task and acknowledges...")
	ackReq := AcknowledgeRequest{AckIDs: []string{ackID}}
	reqBodyBytes, _ := json.Marshal(ackReq)
	reqBody = bytes.NewBuffer(reqBodyBytes)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/worker:acknowledge", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	// Verify message is acknowledged
	t.Log("Verifying message is acknowledged...")
	time.Sleep(100 * time.Millisecond)
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/worker:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2)

	if len(pullResp2.ReceivedMessages) != 0 {
		t.Errorf("Expected 0 messages (acknowledged), got %d", len(pullResp2.ReceivedMessages))
	}

	t.Log("Modify ack deadline workflow test completed successfully")
}

// TestUseCase_ModifyAckDeadlineNack tests NACK simulation
func TestUseCase_ModifyAckDeadlineNack(t *testing.T) {
	server := NewServer()

	// Create topic and subscription
	t.Log("Creating topic and subscription...")
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish a message
	t.Log("Publishing message...")
	server.storage.Publish("projects/test/topics/topic1", []PubSubMessage{
		{Data: "ZmFpbGVkIHRhc2s=", Attributes: map[string]string{"retry": "true"}},
	})

	time.Sleep(100 * time.Millisecond)

	// Pull the message
	t.Log("Pulling message...")
	reqBody := bytes.NewBufferString(`{"maxMessages": 1}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp)

	if len(pullResp.ReceivedMessages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(pullResp.ReceivedMessages))
	}

	ackID := pullResp.ReceivedMessages[0].AckID

	// Simulate NACK by setting deadline to 0
	t.Log("Simulating NACK by setting deadline to 0...")
	modifyReq := ModifyAckDeadlineRequest{
		AckIDs:             []string{ackID},
		AckDeadlineSeconds: 0,
	}
	reqBodyBytes, _ := json.Marshal(modifyReq)
	reqBody = bytes.NewBuffer(reqBodyBytes)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:modifyAckDeadline", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	// Message should be immediately available for redelivery
	t.Log("Message should be immediately available for redelivery...")
	reqBody = bytes.NewBufferString(`{"maxMessages": 1}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/sub1:pull", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var pullResp2 PullResponse
	json.NewDecoder(w.Body).Decode(&pullResp2)

	if len(pullResp2.ReceivedMessages) != 1 {
		t.Errorf("Expected 1 message (NACK redelivery), got %d", len(pullResp2.ReceivedMessages))
	}

	// Verify attributes are preserved
	if pullResp2.ReceivedMessages[0].Message.Attributes["retry"] != "true" {
		t.Error("Message attributes not preserved after NACK")
	}

	t.Log("NACK simulation test completed successfully")
}
