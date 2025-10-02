package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleCreateSubscription(t *testing.T) {
	server := NewServer()

	// Create topic first
	server.storage.CreateTopic("projects/test/topics/topic1")

	// Create subscription
	reqBody := bytes.NewBufferString(`{"topic": "projects/test/topics/topic1"}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub1", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var sub Subscription
	if err := json.NewDecoder(w.Body).Decode(&sub); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if sub.Name != "projects/test/subscriptions/sub1" {
		t.Errorf("Expected subscription name 'projects/test/subscriptions/sub1', got %s", sub.Name)
	}
	if sub.Topic != "projects/test/topics/topic1" {
		t.Errorf("Expected topic 'projects/test/topics/topic1', got %s", sub.Topic)
	}
}

func TestHandleCreateSubscription_TopicNotFound(t *testing.T) {
	server := NewServer()

	reqBody := bytes.NewBufferString(`{"topic": "projects/test/topics/nonexistent"}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub1", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestHandleCreateSubscription_Duplicate(t *testing.T) {
	server := NewServer()

	// Create topic
	server.storage.CreateTopic("projects/test/topics/topic1")

	// Create subscription first time
	reqBody1 := bytes.NewBufferString(`{"topic": "projects/test/topics/topic1"}`)
	req1 := httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub1", reqBody1)
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	server.ServeHTTP(w1, req1)

	// Try to create again
	reqBody2 := bytes.NewBufferString(`{"topic": "projects/test/topics/topic1"}`)
	req2 := httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub1", reqBody2)
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	server.ServeHTTP(w2, req2)

	if w2.Code != http.StatusConflict {
		t.Errorf("Expected status %d, got %d", http.StatusConflict, w2.Code)
	}
}

func TestHandleGetSubscription(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/subscriptions/sub1", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var sub Subscription
	if err := json.NewDecoder(w.Body).Decode(&sub); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if sub.Name != "projects/test/subscriptions/sub1" {
		t.Errorf("Expected subscription name 'projects/test/subscriptions/sub1', got %s", sub.Name)
	}
}

func TestHandleGetSubscription_NotFound(t *testing.T) {
	server := NewServer()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/subscriptions/nonexistent", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestHandleDeleteSubscription(t *testing.T) {
	server := NewServer()

	// Setup
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/subscriptions/sub1", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected status %d, got %d", http.StatusNoContent, w.Code)
	}

	// Verify subscription is deleted
	_, err := server.storage.GetSubscription("projects/test/subscriptions/sub1")
	if err != ErrSubscriptionNotFound {
		t.Errorf("Expected subscription to be deleted")
	}
}

func TestHandleDeleteSubscription_NotFound(t *testing.T) {
	server := NewServer()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/subscriptions/nonexistent", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}
