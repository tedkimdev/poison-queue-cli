use anyhow::{anyhow, Context};
use log::{info, warn};
use rdkafka::{config::RDKafkaLogLevel, consumer::Consumer, ClientConfig, Message};
use serde_json::from_str;
use tabled::{settings::Rotate, Table};

use crate::{cli::DlqMessage, kafka::{CustomContext, LoggingConsumer, AUTO_OFFSET_RESET, BOOTSTRAP_SERVERS, ENABLE_AUTO_COMMIT, ENABLE_PARTITION_EOF, GROUP_ID, SESSION_TIMEOUT_MS, TIMEOUT_MS}};

pub async fn view_message_by_id(
    brokers: &str,
    group_id: &str,
    topic: &str,
    id: &str,
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

    let mut table_data:Vec<DlqMessage> = vec!();
    loop {
        match consumer.recv().await {
            Err(e) => {
                if let rdkafka::error::KafkaError::PartitionEOF(_) = e {
                    info!("Reached end of partition (EOF)");
                    break
                } else {
                    warn!("Kafka error: {}", e);
                    break; // Exit on other errors
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

                let item = DlqMessage::parse(json);
                if item.id.eq(id) || item.correlation_id.eq(id) {
                    table_data.push(item);
                    break;
                }
            }
        };
    }

    if table_data.is_empty() {
        return Err(anyhow!("Message not found"));
    }

    let mut table = Table::new(table_data);
    table.with(Rotate::Left);
    println!("{table}");
    Ok(())
}

