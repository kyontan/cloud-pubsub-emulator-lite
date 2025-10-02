# cloud-pubsub-emulator-lite

Lightweight in-memory Google Cloud Pub/Sub emulator for local development and testing.

## Features

**Supported APIs:**
- Topics: Create, Get, Delete, List, Publish
- Subscriptions: Create, Get, Delete, List, Pull, Acknowledge, ModifyAckDeadline

**Characteristics:**
- HTTP API only (no gRPC)
- In-memory storage (non-persistent)
- No authentication/authorization
- Single-process emulator

**Not Supported:**
- Subscription filters, dead letter topics, ordering keys
- Push subscriptions, snapshots, schemas
- IAM, streaming pull, exponential backoff

## Installation

```bash
go build -o pubsub-emulator
```

## Usage

```bash
# Default (all interfaces, port 8085)
./pubsub-emulator

# Custom port
./pubsub-emulator -p 9090

# Custom host and port
./pubsub-emulator -h localhost -p 9090

# Health check
curl http://localhost:8085/health
```

## API Examples

```bash
# Create topic
curl -X PUT http://localhost:8085/v1/projects/myproject/topics/mytopic

# Create subscription
curl -X PUT http://localhost:8085/v1/projects/myproject/subscriptions/mysub \
  -H "Content-Type: application/json" \
  -d '{"topic": "projects/myproject/topics/mytopic"}'

# Publish messages (data must be base64 encoded)
curl -X POST http://localhost:8085/v1/projects/myproject/topics/mytopic:publish \
  -H "Content-Type: application/json" \
  -d '{"messages": [{"data": "SGVsbG8gV29ybGQ=", "attributes": {"key": "value"}}]}'

# Pull messages
curl -X POST http://localhost:8085/v1/projects/myproject/subscriptions/mysub:pull \
  -H "Content-Type: application/json" \
  -d '{"maxMessages": 10}'

# Acknowledge messages
curl -X POST http://localhost:8085/v1/projects/myproject/subscriptions/mysub:acknowledge \
  -H "Content-Type: application/json" \
  -d '{"ackIds": ["ack-id-1"]}'

# Modify ack deadline (0 = immediate redelivery/NACK)
curl -X POST http://localhost:8085/v1/projects/myproject/subscriptions/mysub:modifyAckDeadline \
  -H "Content-Type: application/json" \
  -d '{"ackIds": ["ack-id-1"], "ackDeadlineSeconds": 60}'

# List topics
curl -X GET http://localhost:8085/v1/projects/myproject/topics

# List subscriptions
curl -X GET http://localhost:8085/v1/projects/myproject/subscriptions

# Delete resources
curl -X DELETE http://localhost:8085/v1/projects/myproject/subscriptions/mysub
curl -X DELETE http://localhost:8085/v1/projects/myproject/topics/mytopic
```

## Testing

```bash
# Run all tests
go test -v ./...

# Run specific test
go test -v -run TestUseCase_BasicPubSub
```
