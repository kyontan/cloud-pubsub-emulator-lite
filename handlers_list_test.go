package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleListTopics_Empty(t *testing.T) {
	server := NewServer()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/topics", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp ListTopicsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.Topics) != 0 {
		t.Errorf("Expected 0 topics, got %d", len(resp.Topics))
	}
}

func TestHandleListTopics_WithTopics(t *testing.T) {
	server := NewServer()

	// Create some topics
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateTopic("projects/test/topics/topic2")
	server.storage.CreateTopic("projects/other/topics/topic3")

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/topics", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp ListTopicsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.Topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(resp.Topics))
	}

	// Verify topics are from the correct project
	for _, topic := range resp.Topics {
		if topic.Name != "projects/test/topics/topic1" && topic.Name != "projects/test/topics/topic2" {
			t.Errorf("Unexpected topic name: %s", topic.Name)
		}
	}
}

func TestHandleListTopics_MethodNotAllowed(t *testing.T) {
	server := NewServer()

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleListSubscriptions_Empty(t *testing.T) {
	server := NewServer()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/subscriptions", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp ListSubscriptionsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.Subscriptions) != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", len(resp.Subscriptions))
	}
}

func TestHandleListSubscriptions_WithSubscriptions(t *testing.T) {
	server := NewServer()

	// Create topic and subscriptions
	server.storage.CreateTopic("projects/test/topics/topic1")
	server.storage.CreateTopic("projects/other/topics/topic2")
	server.storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/test/subscriptions/sub2", "projects/test/topics/topic1")
	server.storage.CreateSubscription("projects/other/subscriptions/sub3", "projects/other/topics/topic2")

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/subscriptions", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp ListSubscriptionsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.Subscriptions) != 2 {
		t.Errorf("Expected 2 subscriptions, got %d", len(resp.Subscriptions))
	}

	// Verify subscriptions are from the correct project
	for _, sub := range resp.Subscriptions {
		if sub.Name != "projects/test/subscriptions/sub1" && sub.Name != "projects/test/subscriptions/sub2" {
			t.Errorf("Unexpected subscription name: %s", sub.Name)
		}
	}
}

func TestHandleListSubscriptions_MethodNotAllowed(t *testing.T) {
	server := NewServer()

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/subscriptions", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleListTopics_MultipleProjects(t *testing.T) {
	server := NewServer()

	// Create topics in different projects
	server.storage.CreateTopic("projects/project-a/topics/topic1")
	server.storage.CreateTopic("projects/project-a/topics/topic2")
	server.storage.CreateTopic("projects/project-b/topics/topic1")
	server.storage.CreateTopic("projects/project-b/topics/topic2")
	server.storage.CreateTopic("projects/project-b/topics/topic3")

	// List topics for project-a
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/project-a/topics", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var respA ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&respA)

	if len(respA.Topics) != 2 {
		t.Errorf("Expected 2 topics for project-a, got %d", len(respA.Topics))
	}

	// List topics for project-b
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/project-b/topics", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var respB ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&respB)

	if len(respB.Topics) != 3 {
		t.Errorf("Expected 3 topics for project-b, got %d", len(respB.Topics))
	}
}

func TestHandleListSubscriptions_MultipleProjects(t *testing.T) {
	server := NewServer()

	// Create topics and subscriptions in different projects
	server.storage.CreateTopic("projects/project-a/topics/topic1")
	server.storage.CreateTopic("projects/project-b/topics/topic1")

	server.storage.CreateSubscription("projects/project-a/subscriptions/sub1", "projects/project-a/topics/topic1")
	server.storage.CreateSubscription("projects/project-a/subscriptions/sub2", "projects/project-a/topics/topic1")
	server.storage.CreateSubscription("projects/project-b/subscriptions/sub1", "projects/project-b/topics/topic1")

	// List subscriptions for project-a
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/project-a/subscriptions", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var respA ListSubscriptionsResponse
	json.NewDecoder(w.Body).Decode(&respA)

	if len(respA.Subscriptions) != 2 {
		t.Errorf("Expected 2 subscriptions for project-a, got %d", len(respA.Subscriptions))
	}

	// List subscriptions for project-b
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/project-b/subscriptions", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var respB ListSubscriptionsResponse
	json.NewDecoder(w.Body).Decode(&respB)

	if len(respB.Subscriptions) != 1 {
		t.Errorf("Expected 1 subscription for project-b, got %d", len(respB.Subscriptions))
	}
}
