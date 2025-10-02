package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	listTopicsRegex       = regexp.MustCompile(`^/v1/projects/([^/]+)/topics$`)
	listSubscriptionsRegex = regexp.MustCompile(`^/v1/projects/([^/]+)/subscriptions$`)
	topicPathRegex        = regexp.MustCompile(`^/v1/projects/([^/]+)/topics/([^/]+)$`)
	topicPublishRegex     = regexp.MustCompile(`^/v1/projects/([^/]+)/topics/([^/]+):publish$`)
	subscriptionPathRegex = regexp.MustCompile(`^/v1/projects/([^/]+)/subscriptions/([^/]+)$`)
	subscriptionPullRegex = regexp.MustCompile(`^/v1/projects/([^/]+)/subscriptions/([^/]+):pull$`)
	subscriptionAckRegex  = regexp.MustCompile(`^/v1/projects/([^/]+)/subscriptions/([^/]+):acknowledge$`)

	logger *slog.Logger
)

func init() {
	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

// Server wraps the storage and provides HTTP handlers
type Server struct {
	storage *Storage
}

// NewServer creates a new Server instance
func NewServer() *Server {
	return &Server{
		storage: NewStorage(),
	}
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// Topic publish (check before topic operations)
	if matches := topicPublishRegex.FindStringSubmatch(path); matches != nil {
		project, topic := matches[1], matches[2]
		topicName := fmt.Sprintf("projects/%s/topics/%s", project, topic)

		if r.Method == http.MethodPost {
			s.handlePublish(w, r, topicName)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// Subscription pull (check before subscription operations)
	if matches := subscriptionPullRegex.FindStringSubmatch(path); matches != nil {
		project, subscription := matches[1], matches[2]
		subscriptionName := fmt.Sprintf("projects/%s/subscriptions/%s", project, subscription)

		if r.Method == http.MethodPost {
			s.handlePull(w, r, subscriptionName)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// Subscription acknowledge (check before subscription operations)
	if matches := subscriptionAckRegex.FindStringSubmatch(path); matches != nil {
		project, subscription := matches[1], matches[2]
		subscriptionName := fmt.Sprintf("projects/%s/subscriptions/%s", project, subscription)

		if r.Method == http.MethodPost {
			s.handleAcknowledge(w, r, subscriptionName)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// List topics (check before specific topic operations)
	if matches := listTopicsRegex.FindStringSubmatch(path); matches != nil {
		projectID := matches[1]

		if r.Method == http.MethodGet {
			s.handleListTopics(w, r, projectID)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// List subscriptions (check before specific subscription operations)
	if matches := listSubscriptionsRegex.FindStringSubmatch(path); matches != nil {
		projectID := matches[1]

		if r.Method == http.MethodGet {
			s.handleListSubscriptions(w, r, projectID)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// Topic operations
	if matches := topicPathRegex.FindStringSubmatch(path); matches != nil {
		project, topic := matches[1], matches[2]
		topicName := fmt.Sprintf("projects/%s/topics/%s", project, topic)

		switch r.Method {
		case http.MethodPut:
			s.handleCreateTopic(w, r, topicName)
		case http.MethodGet:
			s.handleGetTopic(w, r, topicName)
		case http.MethodDelete:
			s.handleDeleteTopic(w, r, topicName)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// Subscription operations
	if matches := subscriptionPathRegex.FindStringSubmatch(path); matches != nil {
		project, subscription := matches[1], matches[2]
		subscriptionName := fmt.Sprintf("projects/%s/subscriptions/%s", project, subscription)

		switch r.Method {
		case http.MethodPut:
			s.handleCreateSubscription(w, r, subscriptionName)
		case http.MethodGet:
			s.handleGetSubscription(w, r, subscriptionName)
		case http.MethodDelete:
			s.handleDeleteSubscription(w, r, subscriptionName)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	http.NotFound(w, r)
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request, topicName string) {
	topic, err := s.storage.CreateTopic(topicName)
	if err != nil {
		logger.Error("failed to create topic",
			"operation", "create_topic",
			"topic", topicName,
			"error", err.Error())
		if err == ErrTopicAlreadyExists {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	logger.Info("topic created",
		"operation", "create_topic",
		"topic", topicName)
	writeJSON(w, http.StatusOK, topic)
}

func (s *Server) handleGetTopic(w http.ResponseWriter, r *http.Request, topicName string) {
	topic, err := s.storage.GetTopic(topicName)
	if err != nil {
		if err == ErrTopicNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	writeJSON(w, http.StatusOK, topic)
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request, topicName string) {
	err := s.storage.DeleteTopic(topicName)
	if err != nil {
		logger.Error("failed to delete topic",
			"operation", "delete_topic",
			"topic", topicName,
			"error", err.Error())
		if err == ErrTopicNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	logger.Info("topic deleted",
		"operation", "delete_topic",
		"topic", topicName)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request, subscriptionName string) {
	var req struct {
		Topic string `json:"topic"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Error("invalid request body",
			"operation", "create_subscription",
			"subscription", subscriptionName,
			"error", err.Error())
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	subscription, err := s.storage.CreateSubscription(subscriptionName, req.Topic)
	if err != nil {
		logger.Error("failed to create subscription",
			"operation", "create_subscription",
			"subscription", subscriptionName,
			"topic", req.Topic,
			"error", err.Error())
		if err == ErrSubscriptionAlreadyExists {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		} else if err == ErrTopicNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	logger.Info("subscription created",
		"operation", "create_subscription",
		"subscription", subscriptionName,
		"topic", req.Topic)
	writeJSON(w, http.StatusOK, subscription)
}

func (s *Server) handleGetSubscription(w http.ResponseWriter, r *http.Request, subscriptionName string) {
	subscription, err := s.storage.GetSubscription(subscriptionName)
	if err != nil {
		if err == ErrSubscriptionNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	writeJSON(w, http.StatusOK, subscription)
}

func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request, subscriptionName string) {
	err := s.storage.DeleteSubscription(subscriptionName)
	if err != nil {
		logger.Error("failed to delete subscription",
			"operation", "delete_subscription",
			"subscription", subscriptionName,
			"error", err.Error())
		if err == ErrSubscriptionNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	logger.Info("subscription deleted",
		"operation", "delete_subscription",
		"subscription", subscriptionName)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request, topicName string) {
	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Error("invalid request body",
			"operation", "publish",
			"topic", topicName,
			"error", err.Error())
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	messageIDs, err := s.storage.Publish(topicName, req.Messages)
	if err != nil {
		logger.Error("failed to publish",
			"operation", "publish",
			"topic", topicName,
			"message_count", len(req.Messages),
			"error", err.Error())
		if err == ErrTopicNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	logger.Info("published",
		"operation", "publish",
		"topic", topicName,
		"message_count", len(messageIDs),
		"message_ids", messageIDs)
	writeJSON(w, http.StatusOK, PublishResponse{MessageIDs: messageIDs})
}

func (s *Server) handlePull(w http.ResponseWriter, r *http.Request, subscriptionName string) {
	var req PullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Error("invalid request body",
			"operation", "pull",
			"subscription", subscriptionName,
			"error", err.Error())
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	if req.MaxMessages <= 0 {
		req.MaxMessages = 1
	}

	messages, err := s.storage.Pull(subscriptionName, req.MaxMessages)
	if err != nil {
		logger.Error("failed to pull",
			"operation", "pull",
			"subscription", subscriptionName,
			"max_messages", req.MaxMessages,
			"error", err.Error())
		if err == ErrSubscriptionNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	if messages == nil {
		messages = []ReceivedMessage{}
	}

	logger.Info("pulled",
		"operation", "pull",
		"subscription", subscriptionName,
		"max_messages", req.MaxMessages,
		"message_count", len(messages))
	writeJSON(w, http.StatusOK, PullResponse{ReceivedMessages: messages})
}

func (s *Server) handleAcknowledge(w http.ResponseWriter, r *http.Request, subscriptionName string) {
	var req AcknowledgeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Error("invalid request body",
			"operation", "acknowledge",
			"subscription", subscriptionName,
			"error", err.Error())
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	err := s.storage.Acknowledge(subscriptionName, req.AckIDs)
	if err != nil {
		logger.Error("failed to acknowledge",
			"operation", "acknowledge",
			"subscription", subscriptionName,
			"ack_id_count", len(req.AckIDs),
			"error", err.Error())
		if err == ErrSubscriptionNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		} else if strings.Contains(err.Error(), "no messages") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	logger.Info("acknowledged",
		"operation", "acknowledge",
		"subscription", subscriptionName,
		"ack_id_count", len(req.AckIDs))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{}"))
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request, projectID string) {
	topics := s.storage.ListTopics()

	// Filter by project if needed (currently we store full names)
	filteredTopics := make([]Topic, 0)
	projectPrefix := fmt.Sprintf("projects/%s/topics/", projectID)
	for _, topic := range topics {
		if strings.HasPrefix(topic.Name, projectPrefix) {
			filteredTopics = append(filteredTopics, *topic)
		}
	}

	logger.Info("listed topics",
		"operation", "list_topics",
		"project", projectID,
		"count", len(filteredTopics))

	writeJSON(w, http.StatusOK, ListTopicsResponse{Topics: filteredTopics})
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request, projectID string) {
	subscriptions := s.storage.ListSubscriptions()

	// Filter by project if needed
	filteredSubs := make([]Subscription, 0)
	projectPrefix := fmt.Sprintf("projects/%s/subscriptions/", projectID)
	for _, sub := range subscriptions {
		if strings.HasPrefix(sub.Name, projectPrefix) {
			filteredSubs = append(filteredSubs, *sub)
		}
	}

	logger.Info("listed subscriptions",
		"operation", "list_subscriptions",
		"project", projectID,
		"count", len(filteredSubs))

	writeJSON(w, http.StatusOK, ListSubscriptionsResponse{Subscriptions: filteredSubs})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// HealthCheck handler
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/health" {
		http.NotFound(w, r)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// parsePort extracts port from address string
func parsePort(addr string) int {
	parts := strings.Split(addr, ":")
	if len(parts) < 2 {
		return 8085
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return 8085
	}
	return port
}
