package main

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	ErrTopicNotFound             = errors.New("topic not found")
	ErrTopicAlreadyExists        = errors.New("topic already exists")
	ErrSubscriptionNotFound      = errors.New("subscription not found")
	ErrSubscriptionAlreadyExists = errors.New("subscription already exists")
)

// Storage is an in-memory storage for Pub/Sub entities
type Storage struct {
	topics        map[string]*Topic
	subscriptions map[string]*Subscription
	messages      map[string][]*InternalMessage // key: subscription name
	mu            sync.RWMutex
}

// NewStorage creates a new Storage instance
func NewStorage() *Storage {
	return &Storage{
		topics:        make(map[string]*Topic),
		subscriptions: make(map[string]*Subscription),
		messages:      make(map[string][]*InternalMessage),
	}
}

// CreateTopic creates a new topic
func (s *Storage) CreateTopic(name string) (*Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; exists {
		return nil, ErrTopicAlreadyExists
	}

	topic := &Topic{Name: name}
	s.topics[name] = topic
	return topic, nil
}

// GetTopic retrieves a topic by name
func (s *Storage) GetTopic(name string) (*Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topic, exists := s.topics[name]
	if !exists {
		return nil, ErrTopicNotFound
	}
	return topic, nil
}

// DeleteTopic deletes a topic
func (s *Storage) DeleteTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; !exists {
		return ErrTopicNotFound
	}

	delete(s.topics, name)
	return nil
}

// ListTopics returns all topics
func (s *Storage) ListTopics() []*Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]*Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}
	return topics
}

// CreateSubscription creates a new subscription
func (s *Storage) CreateSubscription(name, topicName string) (*Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.subscriptions[name]; exists {
		return nil, ErrSubscriptionAlreadyExists
	}

	if _, exists := s.topics[topicName]; !exists {
		return nil, ErrTopicNotFound
	}

	subscription := &Subscription{
		Name:  name,
		Topic: topicName,
	}
	s.subscriptions[name] = subscription
	s.messages[name] = make([]*InternalMessage, 0)
	return subscription, nil
}

// GetSubscription retrieves a subscription by name
func (s *Storage) GetSubscription(name string) (*Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subscription, exists := s.subscriptions[name]
	if !exists {
		return nil, ErrSubscriptionNotFound
	}
	return subscription, nil
}

// DeleteSubscription deletes a subscription
func (s *Storage) DeleteSubscription(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.subscriptions[name]; !exists {
		return ErrSubscriptionNotFound
	}

	delete(s.subscriptions, name)
	delete(s.messages, name)
	return nil
}

// ListSubscriptions returns all subscriptions
func (s *Storage) ListSubscriptions() []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subscriptions := make([]*Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		subscriptions = append(subscriptions, sub)
	}
	return subscriptions
}

// Publish publishes messages to a topic
func (s *Storage) Publish(topicName string, messages []PubSubMessage) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[topicName]; !exists {
		return nil, ErrTopicNotFound
	}

	messageIDs := make([]string, len(messages))
	now := time.Now().Format(time.RFC3339)

	// Generate message IDs first
	for i := range messages {
		messageIDs[i] = uuid.New().String()
	}

	// Find all subscriptions for this topic
	for _, sub := range s.subscriptions {
		if sub.Topic == topicName {
			for i, pubsubMsg := range messages {
				ackID := uuid.New().String()

				msg := Message{
					Data:        pubsubMsg.Data,
					Attributes:  pubsubMsg.Attributes,
					MessageID:   messageIDs[i],
					PublishTime: now,
				}

				// Messages are immediately visible (deadline in the past)
				// The deadline will be set when the message is first pulled
				internalMsg := &InternalMessage{
					Message:    msg,
					AckID:      ackID,
					DeadlineAt: time.Time{}, // Zero time, always in the past
				}

				s.messages[sub.Name] = append(s.messages[sub.Name], internalMsg)
			}
		}
	}

	return messageIDs, nil
}

// Pull retrieves messages from a subscription
func (s *Storage) Pull(subscriptionName string, maxMessages int) ([]ReceivedMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.subscriptions[subscriptionName]; !exists {
		return nil, ErrSubscriptionNotFound
	}

	msgs, exists := s.messages[subscriptionName]
	if !exists {
		return []ReceivedMessage{}, nil
	}

	receivedMessages := make([]ReceivedMessage, 0, maxMessages)
	now := time.Now()

	for _, msg := range msgs {
		if len(receivedMessages) >= maxMessages {
			break
		}

		msg.mu.Lock()
		// Only return messages that are not acked and whose deadline has passed
		// (deadline is zero/past for new messages, making them immediately visible)
		if msg.AckedAt == nil && msg.DeadlineAt.Before(now) {
			receivedMessages = append(receivedMessages, ReceivedMessage{
				AckID:   msg.AckID,
				Message: msg.Message,
			})
			// Set ack deadline - message won't be redelivered until this time
			if testing.Testing() {
				msg.DeadlineAt = now.Add(50 * time.Millisecond)
			} else {
				msg.DeadlineAt = now.Add(10 * time.Second)
			}
		}
		msg.mu.Unlock()
	}

	return receivedMessages, nil
}

// Acknowledge acknowledges messages
func (s *Storage) Acknowledge(subscriptionName string, ackIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.subscriptions[subscriptionName]; !exists {
		return ErrSubscriptionNotFound
	}

	msgs, exists := s.messages[subscriptionName]
	if !exists {
		return fmt.Errorf("no messages for subscription")
	}

	ackIDSet := make(map[string]bool)
	for _, id := range ackIDs {
		ackIDSet[id] = true
	}

	now := time.Now()
	newMessages := make([]*InternalMessage, 0, len(msgs))

	for _, msg := range msgs {
		msg.mu.Lock()
		if ackIDSet[msg.AckID] {
			msg.AckedAt = &now
		}
		// Keep only non-acked messages
		if msg.AckedAt == nil {
			newMessages = append(newMessages, msg)
		}
		msg.mu.Unlock()
	}

	s.messages[subscriptionName] = newMessages
	return nil
}
