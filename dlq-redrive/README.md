# axum-webtools-dlq-redrive

A Kafka Dead Letter Queue (DLQ) redrive tool that helps you reprocess failed messages or move old messages to poison topics. This tool is particularly useful for managing error handling workflows in Kafka-based systems.

> Part of the [axum-webtools](../README.md) workspace.

## Crate & Docker image

- https://crates.io/crates/axum-webtools-dlq-redrive
- https://hub.docker.com/r/jslsolucoes/axum-webtools-dlq-redrive

## Installation

Install the DLQ redrive tool binary:

```bash
cargo install axum-webtools-dlq-redrive
```

## Features

- **Message Redriving**: Moves messages from DLQ topics back to target topics for reprocessing
- **Age-based Filtering**: Automatically routes messages older than a threshold to poison topics
- **Status Checking**: View DLQ topic status, partition info, consumer group lag, and watermarks
- **Offset Management**: Tracks consumer group offsets to enable resumable processing
- **Safe Processing**: Commits offsets only after successful message production

## Basic Usage

### Check DLQ Status

View the current state of a DLQ topic and consumer group:

```bash
dlq-redrive status \
  -b "localhost:9092" \
  -s "my-dlq-topic" \
  -g "dlq-consumer-group"
```

Output includes:
- Partition count and details
- Low and high watermarks per partition
- Committed offsets per partition
- Consumer lag (total messages waiting)

### Redrive Messages

Move messages from DLQ to target topic, with automatic poison routing for old messages:

```bash
dlq-redrive redrive \
  -b "localhost:9092" \
  -s "my-dlq-topic" \
  -t "my-main-topic" \
  -p "my-poison-topic" \
  -g "dlq-consumer-group" \
  --max-age-days 5 \
  --max-messages 1000
```

**Parameters:**
- `-b, --bootstrap-server`: Kafka bootstrap servers
- `-s, --source`: Source DLQ topic
- `-t, --target`: Target topic for redriving messages
- `-p, --poison`: Poison topic for old messages
- `-g, --group`: Consumer group ID
- `--max-age-days`: Maximum message age in days (default: 5). Messages older than this go to poison topic
- `--max-messages`: Maximum number of messages to process (default: 0 = all)

## How It Works

1. **Status Command**:
   - Connects to Kafka and fetches metadata for the specified topic
   - Retrieves watermarks (low/high offsets) for each partition
   - Fetches committed offsets for the consumer group
   - Calculates lag per partition and total lag

2. **Redrive Command**:
   - Subscribes to the source DLQ topic using the specified consumer group
   - Polls messages from Kafka with automatic offset tracking
   - Checks message age using `created_at` field in JSON payload
   - Routes messages based on age:
     - **Recent messages** (≤ max-age-days): Sent to target topic for reprocessing
     - **Old messages** (> max-age-days): Sent to poison topic
   - Commits offsets only after successful production
   - Stops when max-messages is reached or no new messages for 5 seconds

## Message Format Requirements

For age-based filtering to work, messages must contain a `created_at` field in ISO 8601 format:

```json
{
  "created_at": "2026-01-31T10:30:00Z",
  "content": "your message data"
}
```

Messages without `created_at` or invalid timestamps are treated as recent messages and sent to the target topic.

## Use Cases

**Perfect for:**
- **DLQ Management**: Reprocess failed messages after fixing bugs
- **Age-based Cleanup**: Archive or discard messages that are too old to process
- **Resumable Processing**: Process large DLQs in batches using max-messages
- **Monitoring**: Check DLQ lag and health before redriving
- **Safe Redriving**: Automatic offset commit ensures messages aren't lost

## Example Workflow

```bash
# 1. Check how many messages are waiting
dlq-redrive status \
  -b "kafka:9092" \
  -s "orders-dlq" \
  -g "orders-dlq-redrive"

# Output:
# Checking DLQ status...
# Source topic: orders-dlq
# Consumer group: orders-dlq-redrive
# Found 3 partitions in orders-dlq
# Partition 0: Low=0, High=150, Committed=0, Lag=150
# Partition 1: Low=0, High=200, Committed=0, Lag=200
# Partition 2: Low=0, High=100, Committed=0, Lag=100
# Total lag: 450 messages

# 2. Redrive first 100 messages as a test
dlq-redrive redrive \
  -b "kafka:9092" \
  -s "orders-dlq" \
  -t "orders" \
  -p "orders-poison" \
  -g "orders-dlq-redrive" \
  --max-age-days 7 \
  --max-messages 100

# Output:
# DLQ redrive process completed successfully!
# Redrove 85 messages from orders-dlq to orders
# Sent 15 messages to orders-poison (older than 7 days)
# Messages will be automatically deleted based on topic retention policies

# 3. After verification, process remaining messages
dlq-redrive redrive \
  -b "kafka:9092" \
  -s "orders-dlq" \
  -t "orders" \
  -p "orders-poison" \
  -g "orders-dlq-redrive" \
  --max-age-days 7
```

## Error Handling

- **Production Failures**: If message production fails, offset is NOT committed, ensuring no message loss
- **Consumer Errors**: Errors are logged to stderr, processing continues
- **Idle Timeout**: Automatically exits after 5 seconds (10 polls) with no new messages
- **Partition EOF**: Gracefully handles reaching end of partitions

## Advanced Features

### Message Key Preservation

The tool automatically preserves Kafka message keys when redriving, maintaining partitioning behavior:

```rust
// Original message key is preserved in redriven message
if let Some(key) = msg.key() {
    record = record.key(key);
}
```

### Resumable Processing

Using consumer groups enables resumable processing:
- Run redrive with `--max-messages 1000`
- Stop and verify results
- Run again with same consumer group to continue from last committed offset

### Monitoring Integration

The status command output can be parsed for monitoring:

```bash
# Get total lag for alerting
dlq-redrive status -b "kafka:9092" -s "my-dlq" -g "my-group" | grep "Total lag"
```

This tool provides a robust solution for managing Kafka DLQs with safety, flexibility, and operational visibility.
