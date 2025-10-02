package main

import (
	"encoding/base64"
	"sync"
	"time"
)

// Topic represents a Pub/Sub topic
type Topic struct {
	Name string `json:"name"`
}

// Subscription represents a Pub/Sub subscription
type Subscription struct {
	Name  string `json:"name"`
	Topic string `json:"topic"`
}

// Message represents a Pub/Sub message
type Message struct {
	Data       string            `json:"data"`       // base64 encoded
	Attributes map[string]string `json:"attributes"` // optional
	MessageID  string            `json:"messageId"`
	PublishTime string           `json:"publishTime"`
}

// ReceivedMessage wraps a message with an ackId for pulling
type ReceivedMessage struct {
	AckID   string  `json:"ackId"`
	Message Message `json:"message"`
}

// PubSubMessage is used for publishing
type PubSubMessage struct {
	Data       string            `json:"data"`       // base64 encoded
	Attributes map[string]string `json:"attributes"` // optional
}

// PublishRequest is the request body for publishing messages
type PublishRequest struct {
	Messages []PubSubMessage `json:"messages"`
}

// PublishResponse is the response for publishing messages
type PublishResponse struct {
	MessageIDs []string `json:"messageIds"`
}

// PullRequest is the request body for pulling messages
type PullRequest struct {
	MaxMessages int `json:"maxMessages"`
}

// PullResponse is the response for pulling messages
type PullResponse struct {
	ReceivedMessages []ReceivedMessage `json:"receivedMessages"`
}

// AcknowledgeRequest is the request body for acknowledging messages
type AcknowledgeRequest struct {
	AckIDs []string `json:"ackIds"`
}

// ListTopicsResponse is the response for listing topics
type ListTopicsResponse struct {
	Topics []Topic `json:"topics"`
}

// ListSubscriptionsResponse is the response for listing subscriptions
type ListSubscriptionsResponse struct {
	Subscriptions []Subscription `json:"subscriptions"`
}

// InternalMessage represents a message in the storage layer
type InternalMessage struct {
	Message   Message
	AckID     string
	AckedAt   *time.Time
	DeadlineAt time.Time
	mu        sync.Mutex
}

// Encode data to base64
func EncodeData(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// Decode base64 data
func DecodeData(data string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(data)
}
