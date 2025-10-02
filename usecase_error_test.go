package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestUseCase_CreateSubscriptionWithoutTopic tests error handling when creating subscription without topic
func TestUseCase_CreateSubscriptionWithoutTopic(t *testing.T) {
	server := NewServer()

	t.Log("Attempting to create subscription without topic...")
	reqBody := bytes.NewBufferString(`{"topic": "projects/test/topics/nonexistent"}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub1", reqBody)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}

	t.Log("Error handling test completed successfully")
}

// TestUseCase_PublishToNonexistentTopic tests error handling when publishing to nonexistent topic
func TestUseCase_PublishToNonexistentTopic(t *testing.T) {
	server := NewServer()

	t.Log("Attempting to publish to nonexistent topic...")
	reqBody := bytes.NewBufferString(`{"messages": [{"data": "dGVzdA=="}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/nonexistent:publish", reqBody)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}

	t.Log("Publish to nonexistent topic test completed successfully")
}

// TestUseCase_PullFromNonexistentSubscription tests error handling when pulling from nonexistent subscription
func TestUseCase_PullFromNonexistentSubscription(t *testing.T) {
	server := NewServer()

	t.Log("Attempting to pull from nonexistent subscription...")
	reqBody := bytes.NewBufferString(`{"maxMessages": 1}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions/nonexistent:pull", reqBody)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}

	t.Log("Pull from nonexistent subscription test completed successfully")
}

// TestUseCase_DuplicateTopicCreation tests error handling when creating duplicate topic
func TestUseCase_DuplicateTopicCreation(t *testing.T) {
	server := NewServer()

	// Create topic
	t.Log("Creating topic...")
	req := httptest.NewRequest(http.MethodPut, "/v1/projects/test/topics/topic1", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Failed to create initial topic: %d", w.Code)
	}

	// Try to create duplicate
	t.Log("Attempting to create duplicate topic...")
	req = httptest.NewRequest(http.MethodPut, "/v1/projects/test/topics/topic1", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("Expected status %d, got %d", http.StatusConflict, w.Code)
	}

	t.Log("Duplicate topic creation test completed successfully")
}

// TestUseCase_DuplicateSubscriptionCreation tests error handling when creating duplicate subscription
func TestUseCase_DuplicateSubscriptionCreation(t *testing.T) {
	server := NewServer()

	// Create topic
	t.Log("Creating topic...")
	server.storage.CreateTopic("projects/test/topics/topic1")

	// Create subscription
	t.Log("Creating subscription...")
	reqBody := bytes.NewBufferString(`{"topic": "projects/test/topics/topic1"}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub1", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Failed to create initial subscription: %d", w.Code)
	}

	// Try to create duplicate
	t.Log("Attempting to create duplicate subscription...")
	reqBody = bytes.NewBufferString(`{"topic": "projects/test/topics/topic1"}`)
	req = httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub1", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("Expected status %d, got %d", http.StatusConflict, w.Code)
	}

	t.Log("Duplicate subscription creation test completed successfully")
}

// TestUseCase_InvalidRequestBody tests error handling with malformed JSON
func TestUseCase_InvalidRequestBody(t *testing.T) {
	server := NewServer()

	// Create topic for testing
	server.storage.CreateTopic("projects/test/topics/topic1")

	t.Log("Testing invalid JSON for subscription creation...")
	reqBody := bytes.NewBufferString(`{invalid json}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/projects/test/subscriptions/sub1", reqBody)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d for invalid JSON, got %d", http.StatusBadRequest, w.Code)
	}

	t.Log("Testing invalid JSON for publish...")
	reqBody = bytes.NewBufferString(`{invalid json}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1:publish", reqBody)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d for invalid JSON, got %d", http.StatusBadRequest, w.Code)
	}

	t.Log("Invalid request body test completed successfully")
}

// TestUseCase_UnsupportedHTTPMethod tests error handling for unsupported HTTP methods
func TestUseCase_UnsupportedHTTPMethod(t *testing.T) {
	server := NewServer()

	t.Log("Testing POST on topic path (should be PUT)...")
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/topic1", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}

	t.Log("Testing GET on publish path (should be POST)...")
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/topics/topic1:publish", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}

	t.Log("Unsupported HTTP method test completed successfully")
}

// TestUseCase_InvalidPath tests error handling for invalid paths
func TestUseCase_InvalidPath(t *testing.T) {
	server := NewServer()

	t.Log("Testing request to invalid path...")
	req := httptest.NewRequest(http.MethodGet, "/v1/invalid/path", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}

	t.Log("Invalid path test completed successfully")
}
