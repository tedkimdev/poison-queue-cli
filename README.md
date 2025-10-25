# Poison Queue CLI
A command-line interface tool built in Rust for managing poison messages in Apache Kafka. This tool helps analyze and manage problematic messages that repeatedly fail processing, ensuring system reliability and preventing message processing bottlenecks.

## Dead Letter Queue (DLQ) Message Structure

Each message sent to the DLQ is a JSON object with **all operational context stored in the `metadata` field**. This allows consumers or inspectors to handle messages without relying on Kafka headers.

### Example DLQ Message

```json
{
  "correlationId": "dafc5816-268a-442c-9981-1937ed1a4b9f",
  "id": "200103dc-25c9-4678-ae80-f3f1a5833366",
  "metadata": {
    "failureReason": "Max retries",
    "movedToDlqAt": "2025-10-10T10:48:58Z",
    "originalTopic": "user-events",
    "retryCount": 5,
    "source": "test"
  },
  "payload": {
    "action": "test",
    "data": {
      "email": "missing_email",
      "name": "name"
    },
    "userId": "user-123"
  }
}
```

- Metadata contains all operational info: correlation IDs, retry counts, failure reasons, source, original topic, and DLQ timestamps.
- Payload contains only business data
- Kafka headers are optional: since all necessary info is in metadata, headers do not need to be inspected for DLQ management.

### DLQ Message Handling

- Messages are processed in **offset order** (oldest first).  
- After processing a message (archiving/reprocessing), the consumer commits its offset.  
- Committing ensures all earlier messages in the same partition are considered handled.  
- This guarantees no messages are skipped and avoids inconsistencies when archiving or reprocessing.

### Optional Consideration: Database Tracking for DLQ

Kafka DLQ messages are committed sequentially per partition, which means handling a message in the middle can unintentionally skip earlier messages. 

One way to handle messages individually is to maintain a database table that tracks each DLQ message’s status, for example:

- `pending` — message not yet processed  
- `archived` — message safely stored in archive topic  
- `reprocessed` — message sent back to the original topic  
- `discarded` — message intentionally removed  

This approach allows **selective processing, auditing, and safe reprocessing**.  
> Note: This is just a conceptual approach; it is not implemented in this project.

## Usage Example
```
# 1. Check what's in the DLQ
cargo run list-messages dlq-user-events

# Output:
# | id                | reason      | retries | original_topic |
# | f6fcc3f0-f911-... | Max retries | 5       | user-events    |

# 2. View the problematic message
cargo run view-message dlq-user-events f6fcc3f0-f911-4083-a30a-bf72780096be

# 3. Create fixed-payload.json with corrected data
echo '{"userId": "12345", "email": "valid@example.com"}' > fixed-payload.json

# 4. Fix and republish (shows diff, asks confirmation)
cargo run edit-message dlq-user-events f6fcc3f0-f911-4083-a30a-bf72780096be \
    --payload-file fixed-payload.json

# Shows diff, confirms, then:
# - Publishes to user-events (original topic)
# - Removes from dlq-user-events (commits offset)

# 5. Verify it's gone from DLQ
cargo run list-messages dlq-user-events
# (Should be empty or not show that message)
```

## Running Locally

### 1. Start Kafka Infrastructure
`docker-compose up -d`

### 2. Creating test topics
`docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic user-events --partitions 1 --replication-factor 1 2>/dev/null`

`docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic dlq-user-events --partitions 1 --replication-factor 1 2>/dev/null`

### 2.1 Creating archive topic
`docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic dlq-archive --partitions 1 --replication-factor 1 2>/dev/null`


### 3. Send sample normal messages
`./send-message.sh user-events normal`

### 4. Send a sample poison message
`./send-message.sh user-events poison` 

### 5. Send a sample DLQ message
`./send-message.sh dlq-user-events dlq`

### 6. List topics
`docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list`

### 7. Show messages in dlq-user-events
`docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic dlq-user-events --from-beginning --max-messages 10`

`docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic dlq-user-events --from-beginning | jq .`