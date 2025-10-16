use log::{info, warn};
use rdkafka::{config::RDKafkaLogLevel, consumer::Consumer, ClientConfig, Message};
use serde_json::{from_str};
use tabled::{Table, Tabled};

use crate::kafka::{CustomContext, LoggingConsumer};

pub async fn list_messages(
    brokers: &str,
    group_id: &str,
    topic: &str,
    // assignor: Option<&String>,
) -> Result<(), anyhow::Error> {
    let context = CustomContext;

    let mut config = ClientConfig::new();

    config
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "true")  // Enable EOF detection
        .set("auto.offset.reset", "earliest") // Start from beginning
        .set("enable.auto.commit", "false")   // Don't auto-commit
        .set("session.timeout.ms", "6000")
        .set_log_level(RDKafkaLogLevel::Debug);

    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .expect("Consumer creation failed");

    let topics = &[topic];
    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topic");

    let mut table_data:Vec<MessageListItem> = vec!();
    loop {
        match consumer.recv().await {
            Err(e) => {
                if let rdkafka::error::KafkaError::PartitionEOF(_) = e {
                    info!("Reached end of partition (EOF)");
                    break;
                } else {
                    warn!("Kafka error: {}", e);
                    break; // Exit on other errors
                }
            }
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                
                let json: serde_json::Value = from_str(payload).expect("Failed to parse");
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
        let id = json["id"].as_str().unwrap_or("-");
        let correlation_id = json["correlationId"].as_str().unwrap_or("-");
        let reason = json["metadata"]["failureReason"].as_str().unwrap_or("-");
        let retries = json["metadata"]["retryCount"].as_i64().unwrap_or(0);
        let original = json["metadata"]["originalTopic"].as_str().unwrap_or("-");
        let moved_at = json["metadata"]["movedToDlqAt"].as_str().unwrap_or("-");

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