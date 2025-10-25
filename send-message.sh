#!/bin/bash

# Quick message sender with correlation ID and metadata
# Usage: ./send-message.sh <topic> <message-type>

TOPIC=$1
TYPE=${2:-"normal"}

if [ -z "$TOPIC" ]; then
    echo "Usage: $0 <topic-name> [message-type]"
    echo "Message types: normal, poison, dlq"
    exit 1
fi

# Generate UUIDs
ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
CORR_ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"

# Create message based on type
case $TYPE in
    "poison")
        MSG='{"id":"'$ID'","correlationId":"'$CORR_ID'","metadata":{"source":"test","retryCount":3,"failureReason":"Invalid format"},"payload":{"userId":"user-123","action":"test","data":{"email":"invalid","age":"not-number"}}}'
        ;;
    "dlq")
        MSG='{"id":"'$ID'","correlationId":"'$CORR_ID'","metadata":{"source":"test","retryCount":5,"failureReason":"Max retries","originalTopic":"user-events","movedToDlqAt":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"},"payload":{"userId":"user-123","action":"test","data":{"email":"malformed","corrupted":true}}}'
        ;;
    *)
        MSG='{"id":"'$ID'","correlationId":"'$CORR_ID'","metadata":{"source":"test","retryCount":0},"payload":{"userId":"user-123","action":"test","data":{"email":"user@example.com","age":25}}}'
        ;;
esac

echo "Sending $TYPE message to $TOPIC"
echo "ID: $ID, Correlation ID: $CORR_ID"
echo "$MSG" | docker-compose exec -T kafka kafka-console-producer --bootstrap-server localhost:9092 --topic "$TOPIC"
echo "Message sent!"
