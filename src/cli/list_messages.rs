use anyhow::{anyhow, Context};
use log::{info, warn};
use rdkafka::{config::RDKafkaLogLevel, consumer::Consumer, ClientConfig, Message};
use serde_json::{from_str};
use tabled::{Table, Tabled};

use crate::{cli::{FIELD_CORRELATION_ID, FIELD_ID, FIELD_METADATA, METADATA_FAILURE_REASON, METADATA_MOVED_TO_DLQ_AT, METADATA_ORIGINAL_TOPIC, METADATA_RETRY_COUNT}, kafka::{CustomContext, LoggingConsumer, AUTO_OFFSET_RESET, BOOTSTRAP_SERVERS, ENABLE_AUTO_COMMIT, ENABLE_PARTITION_EOF, GROUP_ID, SESSION_TIMEOUT_MS, TIMEOUT_MS}};

pub async fn list_messages(
    brokers: &str,
    group_id: &str,
    topic: &str,
    // assignor: Option<&String>,
) -> Result<(), anyhow::Error> {
    let context = CustomContext;

    let mut config = ClientConfig::new();

    config
        .set(GROUP_ID, group_id)
        .set(BOOTSTRAP_SERVERS, brokers)
        .set(ENABLE_PARTITION_EOF, "true")  // Enable EOF detection
        .set(AUTO_OFFSET_RESET, "earliest") // Start from beginning
        .set(ENABLE_AUTO_COMMIT, "false")   // Don't auto-commit
        .set(SESSION_TIMEOUT_MS, TIMEOUT_MS)
        .set_log_level(RDKafkaLogLevel::Debug);

    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .context("Consumer creation failed")?;

    let topics = &[topic];
    consumer
        .subscribe(topics)
        .map_err(|e| anyhow!("Failed to subscribe to topic: {}", e))?;

    let mut table_data:Vec<MessageListItem> = vec!();
    loop {
        match consumer.recv().await {
            Err(e) => {
                if let rdkafka::error::KafkaError::PartitionEOF(_) = e {
                    info!("Reached end of partition (EOF)");
                    break;
                } else {
                    return Err(anyhow!("Kafka error: {}", e));
                }
            }
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => {
                        warn!("No payload found in message, skipping");
                        continue;
                    },
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                
                let json: serde_json::Value = match from_str(payload) {
                    Ok(j) => j,
                    Err(e) => {
                        warn!("Failed to parse message as JSON: {}, skipping", e);
                        continue;
                    }
                };

                let item = MessageListItem::parse(json);
                table_data.push(item);
            }
        };
    }

    let table = Table::new(table_data);
    println!("{table}");
    Ok(())
}

#[derive(Tabled)]
struct MessageListItem {
    id: String,
    correlation_id: String,
    reason: String,
    retries: i64,
    original_topic: String,
    moved_at: String,
}

impl MessageListItem {
    fn parse(json: serde_json::Value) -> Self {
        let id = json[FIELD_ID].as_str().unwrap_or("-");
        let correlation_id = json[FIELD_CORRELATION_ID].as_str().unwrap_or("-");
        let reason = json[FIELD_METADATA][METADATA_FAILURE_REASON].as_str().unwrap_or("-");
        let retries = json[FIELD_METADATA][METADATA_RETRY_COUNT].as_i64().unwrap_or(0);
        let original = json[FIELD_METADATA][METADATA_ORIGINAL_TOPIC].as_str().unwrap_or("-");
        let moved_at = json[FIELD_METADATA][METADATA_MOVED_TO_DLQ_AT].as_str().unwrap_or("-");

        Self {
            id: id.to_string(),
            correlation_id: correlation_id.to_string(),
            reason: reason.to_string(),
            retries,
            original_topic: original.to_string(),
            moved_at: moved_at.to_string(),
        }
    }
}