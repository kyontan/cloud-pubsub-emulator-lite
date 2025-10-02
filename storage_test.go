package main

import (
	"testing"
	"time"
)

func TestStorage_CreateTopic(t *testing.T) {
	storage := NewStorage()

	// Test creating a topic
	topic, err := storage.CreateTopic("projects/test/topics/topic1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if topic.Name != "projects/test/topics/topic1" {
		t.Errorf("Expected topic name 'projects/test/topics/topic1', got %s", topic.Name)
	}

	// Test creating duplicate topic
	_, err = storage.CreateTopic("projects/test/topics/topic1")
	if err != ErrTopicAlreadyExists {
		t.Errorf("Expected ErrTopicAlreadyExists, got %v", err)
	}
}

func TestStorage_GetTopic(t *testing.T) {
	storage := NewStorage()

	// Test getting non-existent topic
	_, err := storage.GetTopic("projects/test/topics/nonexistent")
	if err != ErrTopicNotFound {
		t.Errorf("Expected ErrTopicNotFound, got %v", err)
	}

	// Create and get topic
	storage.CreateTopic("projects/test/topics/topic1")
	topic, err := storage.GetTopic("projects/test/topics/topic1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if topic.Name != "projects/test/topics/topic1" {
		t.Errorf("Expected topic name 'projects/test/topics/topic1', got %s", topic.Name)
	}
}

func TestStorage_DeleteTopic(t *testing.T) {
	storage := NewStorage()

	// Test deleting non-existent topic
	err := storage.DeleteTopic("projects/test/topics/nonexistent")
	if err != ErrTopicNotFound {
		t.Errorf("Expected ErrTopicNotFound, got %v", err)
	}

	// Create and delete topic
	storage.CreateTopic("projects/test/topics/topic1")
	err = storage.DeleteTopic("projects/test/topics/topic1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify topic is deleted
	_, err = storage.GetTopic("projects/test/topics/topic1")
	if err != ErrTopicNotFound {
		t.Errorf("Expected ErrTopicNotFound after deletion, got %v", err)
	}
}

func TestStorage_ListTopics(t *testing.T) {
	storage := NewStorage()

	// Test empty list
	topics := storage.ListTopics()
	if len(topics) != 0 {
		t.Errorf("Expected 0 topics, got %d", len(topics))
	}

	// Create topics and list
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateTopic("projects/test/topics/topic2")
	topics = storage.ListTopics()
	if len(topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(topics))
	}
}

func TestStorage_CreateSubscription(t *testing.T) {
	storage := NewStorage()

	// Test creating subscription without topic
	_, err := storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")
	if err != ErrTopicNotFound {
		t.Errorf("Expected ErrTopicNotFound, got %v", err)
	}

	// Create topic first
	storage.CreateTopic("projects/test/topics/topic1")

	// Test creating subscription
	sub, err := storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if sub.Name != "projects/test/subscriptions/sub1" {
		t.Errorf("Expected subscription name 'projects/test/subscriptions/sub1', got %s", sub.Name)
	}
	if sub.Topic != "projects/test/topics/topic1" {
		t.Errorf("Expected topic 'projects/test/topics/topic1', got %s", sub.Topic)
	}

	// Test creating duplicate subscription
	_, err = storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")
	if err != ErrSubscriptionAlreadyExists {
		t.Errorf("Expected ErrSubscriptionAlreadyExists, got %v", err)
	}
}

func TestStorage_GetSubscription(t *testing.T) {
	storage := NewStorage()

	// Test getting non-existent subscription
	_, err := storage.GetSubscription("projects/test/subscriptions/nonexistent")
	if err != ErrSubscriptionNotFound {
		t.Errorf("Expected ErrSubscriptionNotFound, got %v", err)
	}

	// Create and get subscription
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")
	sub, err := storage.GetSubscription("projects/test/subscriptions/sub1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if sub.Name != "projects/test/subscriptions/sub1" {
		t.Errorf("Expected subscription name 'projects/test/subscriptions/sub1', got %s", sub.Name)
	}
}

func TestStorage_DeleteSubscription(t *testing.T) {
	storage := NewStorage()

	// Test deleting non-existent subscription
	err := storage.DeleteSubscription("projects/test/subscriptions/nonexistent")
	if err != ErrSubscriptionNotFound {
		t.Errorf("Expected ErrSubscriptionNotFound, got %v", err)
	}

	// Create and delete subscription
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")
	err = storage.DeleteSubscription("projects/test/subscriptions/sub1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify subscription is deleted
	_, err = storage.GetSubscription("projects/test/subscriptions/sub1")
	if err != ErrSubscriptionNotFound {
		t.Errorf("Expected ErrSubscriptionNotFound after deletion, got %v", err)
	}
}

func TestStorage_PublishAndPull(t *testing.T) {
	storage := NewStorage()

	// Setup
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Test publishing to non-existent topic
	_, err := storage.Publish("projects/test/topics/nonexistent", []PubSubMessage{
		{Data: "dGVzdA==", Attributes: map[string]string{"key": "value"}},
	})
	if err != ErrTopicNotFound {
		t.Errorf("Expected ErrTopicNotFound, got %v", err)
	}

	// Publish messages
	messages := []PubSubMessage{
		{Data: "dGVzdDE=", Attributes: map[string]string{"key1": "value1"}},
		{Data: "dGVzdDI=", Attributes: map[string]string{"key2": "value2"}},
	}
	messageIDs, err := storage.Publish("projects/test/topics/topic1", messages)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(messageIDs) != 2 {
		t.Errorf("Expected 2 message IDs, got %d", len(messageIDs))
	}

	// Pull messages immediately (should be available right after publish)
	pulled, err := storage.Pull("projects/test/subscriptions/sub1", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled) != 2 {
		t.Errorf("Expected 2 messages immediately, got %d", len(pulled))
	}
	if pulled[0].Message.Data != "dGVzdDE=" {
		t.Errorf("Expected data 'dGVzdDE=', got %s", pulled[0].Message.Data)
	}
	if pulled[0].Message.Attributes["key1"] != "value1" {
		t.Errorf("Expected attribute key1='value1', got %s", pulled[0].Message.Attributes["key1"])
	}

	// Pull again immediately - should be empty because messages are within ack deadline
	pulled, err = storage.Pull("projects/test/subscriptions/sub1", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled) != 0 {
		t.Errorf("Expected 0 messages (within ack deadline), got %d", len(pulled))
	}

	// Wait for ack deadline to pass
	time.Sleep(100 * time.Millisecond)

	// Pull again - messages should be redelivered since they weren't acked
	pulled, err = storage.Pull("projects/test/subscriptions/sub1", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled) != 2 {
		t.Errorf("Expected 2 messages after deadline, got %d", len(pulled))
	}
}

func TestStorage_PullWithMaxMessages(t *testing.T) {
	storage := NewStorage()

	// Setup
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish 5 messages
	messages := make([]PubSubMessage, 5)
	for i := 0; i < 5; i++ {
		messages[i] = PubSubMessage{Data: "dGVzdA=="}
	}
	storage.Publish("projects/test/topics/topic1", messages)

	// Pull with maxMessages = 3
	pulled, err := storage.Pull("projects/test/subscriptions/sub1", 3)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(pulled))
	}
}

func TestStorage_Acknowledge(t *testing.T) {
	storage := NewStorage()

	// Setup
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish messages
	messages := []PubSubMessage{
		{Data: "dGVzdDE="},
		{Data: "dGVzdDI="},
	}
	storage.Publish("projects/test/topics/topic1", messages)

	// Pull messages
	pulled, err := storage.Pull("projects/test/subscriptions/sub1", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled) != 2 {
		t.Fatalf("Expected 2 messages, got %d", len(pulled))
	}

	// Acknowledge first message
	ackIDs := []string{pulled[0].AckID}
	err = storage.Acknowledge("projects/test/subscriptions/sub1", ackIDs)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Wait for ack deadline to pass
	time.Sleep(100 * time.Millisecond)

	// Pull again - should only get the second message (first was acked)
	pulled, err = storage.Pull("projects/test/subscriptions/sub1", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled) != 1 {
		t.Errorf("Expected 1 message after ack, got %d", len(pulled))
	}
	if pulled[0].Message.Data != "dGVzdDI=" {
		t.Errorf("Expected second message, got %s", pulled[0].Message.Data)
	}
}

func TestStorage_MultipleSubscriptions(t *testing.T) {
	storage := NewStorage()

	// Setup
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub2", "projects/test/topics/topic1")

	// Publish message
	messages := []PubSubMessage{{Data: "dGVzdA=="}}
	storage.Publish("projects/test/topics/topic1", messages)

	// Both subscriptions should receive the message immediately
	pulled1, err := storage.Pull("projects/test/subscriptions/sub1", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled1) != 1 {
		t.Errorf("Expected 1 message in sub1, got %d", len(pulled1))
	}

	pulled2, err := storage.Pull("projects/test/subscriptions/sub2", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled2) != 1 {
		t.Errorf("Expected 1 message in sub2, got %d", len(pulled2))
	}
}

func TestStorage_ModifyAckDeadline(t *testing.T) {
	storage := NewStorage()

	// Setup
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish and pull messages
	messages := []PubSubMessage{{Data: "dGVzdA=="}}
	storage.Publish("projects/test/topics/topic1", messages)
	pulled, _ := storage.Pull("projects/test/subscriptions/sub1", 10)

	// Modify ack deadline to 0 (make immediately available)
	err := storage.ModifyAckDeadline("projects/test/subscriptions/sub1", []string{pulled[0].AckID}, 0)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Pull immediately - message should be available
	pulled2, err := storage.Pull("projects/test/subscriptions/sub1", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled2) != 1 {
		t.Errorf("Expected 1 message after modifying deadline to 0, got %d", len(pulled2))
	}
}

func TestStorage_ModifyAckDeadline_ExtendDeadline(t *testing.T) {
	storage := NewStorage()

	// Setup
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish and pull messages
	messages := []PubSubMessage{{Data: "dGVzdA=="}}
	storage.Publish("projects/test/topics/topic1", messages)
	pulled, _ := storage.Pull("projects/test/subscriptions/sub1", 10)

	// Modify ack deadline to 10 seconds
	err := storage.ModifyAckDeadline("projects/test/subscriptions/sub1", []string{pulled[0].AckID}, 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Wait for original deadline to pass (50ms in tests)
	time.Sleep(100 * time.Millisecond)

	// Pull - message should NOT be available (extended deadline)
	pulled2, err := storage.Pull("projects/test/subscriptions/sub1", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled2) != 0 {
		t.Errorf("Expected 0 messages (deadline extended), got %d", len(pulled2))
	}
}

func TestStorage_ModifyAckDeadline_SubscriptionNotFound(t *testing.T) {
	storage := NewStorage()

	err := storage.ModifyAckDeadline("projects/test/subscriptions/nonexistent", []string{"test-ack-id"}, 30)
	if err != ErrSubscriptionNotFound {
		t.Errorf("Expected ErrSubscriptionNotFound, got %v", err)
	}
}

func TestStorage_ModifyAckDeadline_InvalidAckID(t *testing.T) {
	storage := NewStorage()

	// Setup
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Try to modify with invalid ack ID
	err := storage.ModifyAckDeadline("projects/test/subscriptions/sub1", []string{"invalid-ack-id"}, 30)
	if err == nil {
		t.Error("Expected error for invalid ack ID, got nil")
	}
}

func TestStorage_ModifyAckDeadline_MultipleMessages(t *testing.T) {
	storage := NewStorage()

	// Setup
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish multiple messages
	messages := []PubSubMessage{
		{Data: "dGVzdDE="},
		{Data: "dGVzdDI="},
		{Data: "dGVzdDM="},
	}
	storage.Publish("projects/test/topics/topic1", messages)
	pulled, _ := storage.Pull("projects/test/subscriptions/sub1", 10)

	// Modify deadline for first and third message
	ackIDs := []string{pulled[0].AckID, pulled[2].AckID}
	err := storage.ModifyAckDeadline("projects/test/subscriptions/sub1", ackIDs, 0)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Pull immediately - should get 2 messages
	pulled2, err := storage.Pull("projects/test/subscriptions/sub1", 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(pulled2) != 2 {
		t.Errorf("Expected 2 messages (modified deadline), got %d", len(pulled2))
	}
}

func TestStorage_ModifyAckDeadline_AfterAcknowledge(t *testing.T) {
	storage := NewStorage()

	// Setup
	storage.CreateTopic("projects/test/topics/topic1")
	storage.CreateSubscription("projects/test/subscriptions/sub1", "projects/test/topics/topic1")

	// Publish, pull, and acknowledge
	messages := []PubSubMessage{{Data: "dGVzdA=="}}
	storage.Publish("projects/test/topics/topic1", messages)
	pulled, _ := storage.Pull("projects/test/subscriptions/sub1", 10)
	storage.Acknowledge("projects/test/subscriptions/sub1", []string{pulled[0].AckID})

	// Try to modify ack deadline after acknowledge
	err := storage.ModifyAckDeadline("projects/test/subscriptions/sub1", []string{pulled[0].AckID}, 30)
	if err == nil {
		t.Error("Expected error when modifying deadline of acknowledged message, got nil")
	}
}
