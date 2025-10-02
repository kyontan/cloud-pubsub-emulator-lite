package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleCreateTopic(t *testing.T) {
	server := NewServer()

	req := httptest.NewRequest(http.MethodPut, "/v1/projects/test/topics/topic1", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var topic Topic
	if err := json.NewDecoder(w.Body).Decode(&topic); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if topic.Name != "projects/test/topics/topic1" {
		t.Errorf("Expected topic name 'projects/test/topics/topic1', got %s", topic.Name)
	}
}

func TestHandleCreateTopic_Duplicate(t *testing.T) {
	server := NewServer()

	// Create topic first time
	req1 := httptest.NewRequest(http.MethodPut, "/v1/projects/test/topics/topic1", nil)
	w1 := httptest.NewRecorder()
	server.ServeHTTP(w1, req1)

	// Try to create again
	req2 := httptest.NewRequest(http.MethodPut, "/v1/projects/test/topics/topic1", nil)
	w2 := httptest.NewRecorder()
	server.ServeHTTP(w2, req2)

	if w2.Code != http.StatusConflict {
		t.Errorf("Expected status %d, got %d", http.StatusConflict, w2.Code)
	}
}

func TestHandleGetTopic(t *testing.T) {
	server := NewServer()

	// Create topic
	server.storage.CreateTopic("projects/test/topics/topic1")

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/topics/topic1", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var topic Topic
	if err := json.NewDecoder(w.Body).Decode(&topic); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if topic.Name != "projects/test/topics/topic1" {
		t.Errorf("Expected topic name 'projects/test/topics/topic1', got %s", topic.Name)
	}
}

func TestHandleGetTopic_NotFound(t *testing.T) {
	server := NewServer()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/topics/nonexistent", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestHandleDeleteTopic(t *testing.T) {
	server := NewServer()

	// Create topic
	server.storage.CreateTopic("projects/test/topics/topic1")

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/topics/topic1", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected status %d, got %d", http.StatusNoContent, w.Code)
	}

	// Verify topic is deleted
	_, err := server.storage.GetTopic("projects/test/topics/topic1")
	if err != ErrTopicNotFound {
		t.Errorf("Expected topic to be deleted")
	}
}

func TestHandleDeleteTopic_NotFound(t *testing.T) {
	server := NewServer()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/topics/nonexistent", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}
