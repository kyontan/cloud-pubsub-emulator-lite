package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestUseCase_ListOperationsWorkflow tests a complete workflow with list operations
func TestUseCase_ListOperationsWorkflow(t *testing.T) {
	server := NewServer()

	// Step 1: List topics initially (should be empty)
	t.Log("Listing topics initially...")
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/myproject/topics", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var listResp1 ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&listResp1)
	if len(listResp1.Topics) != 0 {
		t.Errorf("Expected 0 topics initially, got %d", len(listResp1.Topics))
	}

	// Step 2: Create multiple topics
	t.Log("Creating 3 topics...")
	topics := []string{"topic1", "topic2", "topic3"}
	for _, topic := range topics {
		req = httptest.NewRequest(http.MethodPut, "/v1/projects/myproject/topics/"+topic, nil)
		w = httptest.NewRecorder()
		server.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("Failed to create topic %s: %d", topic, w.Code)
		}
	}

	// Step 3: List topics again (should have 3)
	t.Log("Listing topics after creation...")
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/myproject/topics", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var listResp2 ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&listResp2)
	if len(listResp2.Topics) != 3 {
		t.Errorf("Expected 3 topics, got %d", len(listResp2.Topics))
	}

	// Step 4: Create subscriptions
	t.Log("Creating subscriptions...")
	reqBody := bytes.NewBufferString(`{"topic": "projects/myproject/topics/topic1"}`)
	req = httptest.NewRequest(http.MethodPut, "/v1/projects/myproject/subscriptions/sub1", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	reqBody = bytes.NewBufferString(`{"topic": "projects/myproject/topics/topic1"}`)
	req = httptest.NewRequest(http.MethodPut, "/v1/projects/myproject/subscriptions/sub2", reqBody)
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	// Step 5: List subscriptions
	t.Log("Listing subscriptions...")
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/myproject/subscriptions", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var subsResp ListSubscriptionsResponse
	json.NewDecoder(w.Body).Decode(&subsResp)
	if len(subsResp.Subscriptions) != 2 {
		t.Errorf("Expected 2 subscriptions, got %d", len(subsResp.Subscriptions))
	}

	// Step 6: Delete a topic
	t.Log("Deleting topic1...")
	req = httptest.NewRequest(http.MethodDelete, "/v1/projects/myproject/topics/topic1", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	// Step 7: List topics again (should have 2)
	t.Log("Listing topics after deletion...")
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/myproject/topics", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var listResp3 ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&listResp3)
	if len(listResp3.Topics) != 2 {
		t.Errorf("Expected 2 topics after deletion, got %d", len(listResp3.Topics))
	}

	t.Log("List operations workflow completed successfully")
}

// TestUseCase_ListAcrossProjects tests listing resources across different projects
func TestUseCase_ListAcrossProjects(t *testing.T) {
	server := NewServer()

	// Create resources in project-a
	t.Log("Creating resources in project-a...")
	server.storage.CreateTopic("projects/project-a/topics/topic1")
	server.storage.CreateTopic("projects/project-a/topics/topic2")
	server.storage.CreateSubscription("projects/project-a/subscriptions/sub1", "projects/project-a/topics/topic1")

	// Create resources in project-b
	t.Log("Creating resources in project-b...")
	server.storage.CreateTopic("projects/project-b/topics/topic1")
	server.storage.CreateSubscription("projects/project-b/subscriptions/sub1", "projects/project-b/topics/topic1")
	server.storage.CreateSubscription("projects/project-b/subscriptions/sub2", "projects/project-b/topics/topic1")

	// List topics for project-a
	t.Log("Listing topics for project-a...")
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/project-a/topics", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var topicsA ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&topicsA)
	if len(topicsA.Topics) != 2 {
		t.Errorf("Expected 2 topics for project-a, got %d", len(topicsA.Topics))
	}

	// List topics for project-b
	t.Log("Listing topics for project-b...")
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/project-b/topics", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var topicsB ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&topicsB)
	if len(topicsB.Topics) != 1 {
		t.Errorf("Expected 1 topic for project-b, got %d", len(topicsB.Topics))
	}

	// List subscriptions for project-a
	t.Log("Listing subscriptions for project-a...")
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/project-a/subscriptions", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var subsA ListSubscriptionsResponse
	json.NewDecoder(w.Body).Decode(&subsA)
	if len(subsA.Subscriptions) != 1 {
		t.Errorf("Expected 1 subscription for project-a, got %d", len(subsA.Subscriptions))
	}

	// List subscriptions for project-b
	t.Log("Listing subscriptions for project-b...")
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/project-b/subscriptions", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var subsB ListSubscriptionsResponse
	json.NewDecoder(w.Body).Decode(&subsB)
	if len(subsB.Subscriptions) != 2 {
		t.Errorf("Expected 2 subscriptions for project-b, got %d", len(subsB.Subscriptions))
	}

	t.Log("Cross-project list test completed successfully")
}

// TestUseCase_ListAfterCreateAndDelete tests list operations after various CRUD operations
func TestUseCase_ListAfterCreateAndDelete(t *testing.T) {
	server := NewServer()

	// Create 5 topics
	t.Log("Creating 5 topics...")
	for i := 1; i <= 5; i++ {
		topicName := "projects/test/topics/topic" + string(rune('0'+i))
		server.storage.CreateTopic(topicName)
	}

	// List topics
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/topics", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var listResp1 ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&listResp1)
	if len(listResp1.Topics) != 5 {
		t.Errorf("Expected 5 topics, got %d", len(listResp1.Topics))
	}

	// Delete 2 topics
	t.Log("Deleting 2 topics...")
	server.storage.DeleteTopic("projects/test/topics/topic1")
	server.storage.DeleteTopic("projects/test/topics/topic3")

	// List topics again
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/topics", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	var listResp2 ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&listResp2)
	if len(listResp2.Topics) != 3 {
		t.Errorf("Expected 3 topics after deletion, got %d", len(listResp2.Topics))
	}

	t.Log("List after CRUD operations completed successfully")
}

// TestUseCase_EmptyListOperations tests list operations on empty resources
func TestUseCase_EmptyListOperations(t *testing.T) {
	server := NewServer()

	// List topics (empty)
	t.Log("Listing topics (should be empty)...")
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/empty-project/topics", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status OK for empty list, got %d", w.Code)
	}

	var topicsResp ListTopicsResponse
	json.NewDecoder(w.Body).Decode(&topicsResp)
	if len(topicsResp.Topics) != 0 {
		t.Errorf("Expected 0 topics, got %d", len(topicsResp.Topics))
	}

	// List subscriptions (empty)
	t.Log("Listing subscriptions (should be empty)...")
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/empty-project/subscriptions", nil)
	w = httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status OK for empty list, got %d", w.Code)
	}

	var subsResp ListSubscriptionsResponse
	json.NewDecoder(w.Body).Decode(&subsResp)
	if len(subsResp.Subscriptions) != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", len(subsResp.Subscriptions))
	}

	t.Log("Empty list operations completed successfully")
}
