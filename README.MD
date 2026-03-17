# mini-kafka

A lightweight message broker inspired by Apache Kafka, built in Go from scratch. Uses PostgreSQL for durable metadata storage and Redis for high-throughput message queuing.

## Architecture

```
Producers → HTTP API → Broker Service → Redis (message storage)
                              ↓
                         PostgreSQL (topics, partitions, consumer groups, offsets)
                              ↓
                       Retry Worker (background) → DLQ
```

**PostgreSQL** stores all metadata — topics, partitions, consumer groups, consumer offsets, and retry/dead letter records. It is the source of truth for where each consumer group is up to.

**Redis** stores the actual messages as lists, one list per partition (`broker:<topic>:<partition>`). Messages are appended with `RPUSH` on publish and read with `LRANGE` on consume — fast, simple, and non-destructive.

**Retry Worker** runs as a background goroutine. It polls `retry_messages` every 10 seconds, re-publishes failed messages, and moves exhausted messages to the dead letter queue after 5 attempts.

## Getting started

**Prerequisites**
- Docker
- Go 1.22+

**Run**
```bash
make up     # starts Postgres and Redis
make run    # starts the broker
```

## API reference

**Create a topic**
```bash
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "payments", "partition_count": 3}'
```

**Create a consumer group**
```bash
curl -X POST http://localhost:8080/groups \
  -H "Content-Type: application/json" \
  -d '{"name": "billing"}'
```

**Publish a message**
```bash
curl -X POST http://localhost:8080/topics/payments/messages \
  -H "Content-Type: application/json" \
  -d '{"key": "order-123", "payload": "hello from producer"}'
```

Messages are routed to partitions deterministically by hashing the `key` using FNV-32a. The same key always lands on the same partition.

**Consume messages**
```bash
curl "http://localhost:8080/topics/payments/messages?group=billing&partition=1&limit=10"
```

To find which partition a key lands on:
```bash
go run examples/check_partition.go
```

**Retry a failed message**
```bash
curl -X POST http://localhost:8080/topics/payments/messages/retry \
  -H "Content-Type: application/json" \
  -d '{"group_name": "billing", "partition_index": 1, "offset": 0, "payload": "hello from producer"}'
```

## Running tests

```bash
make test
```

Integration tests run against real Postgres and Redis. Make sure `make up` has been run first. Tests are skipped automatically if the environment variables are not set.

## Design decisions

**At-least-once delivery**
The consumer offset is updated before messages are returned. If the offset update fails, an error is returned and the consumer retries — meaning a message may be delivered more than once but never lost. This is a deliberate tradeoff over at-most-once delivery.

**Retry and dead letter queue**
Failed messages are submitted to `POST /topics/{name}/messages/retry` by the consumer. The broker stores them in Postgres with an attempt counter. A background worker re-publishes them on a 10 second interval. After 5 failed attempts the message is marked `dead` — it remains in Postgres indefinitely for operator inspection and manual replay.

**No third party router**
Go 1.22 added native path parameters to `net/http`. No external router library is needed.

**Structured logging**
All logs are emitted as JSON via `log/slog`. In a containerised environment logs are ingested by tools like Datadog or CloudWatch — structured logs make filtering, alerting, and aggregation trivial.

**Schema initialisation**
Tables are created on startup using `CREATE TABLE IF NOT EXISTS`. In a production system this would be replaced with a versioned migration tool like `goose` or `golang-migrate`.
