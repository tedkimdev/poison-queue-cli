use std::time::Duration;

use anyhow::{anyhow, Context};
use log::{info, warn};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer},
    message::BorrowedMessage,
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use serde_json::{from_str, Value};
use tabled::{settings::Rotate, Table};

use crate::{
    cli::{FIELD_ID, FIELD_METADATA, METADATA_ARCHIVED_AT}, kafka::{
        CustomContext, LoggingConsumer, AUTO_OFFSET_RESET, BOOTSTRAP_SERVERS, ENABLE_AUTO_COMMIT,
        ENABLE_PARTITION_EOF, GROUP_ID, MESSAGE_TIMEOUT_MS, SESSION_TIMEOUT_MS, TIMEOUT_MS,
    }, DlqMessage
};

pub async fn archive_message(
    brokers: &str,
    group_id: &str,
    topic: &str,
    id: &str,
    archive_topic: &str,
) -> Result<(), anyhow::Error> {
    let context = CustomContext;
    let mut config = ClientConfig::new();

    config
        .set(GROUP_ID, group_id)
        .set(BOOTSTRAP_SERVERS, brokers)
        .set(ENABLE_PARTITION_EOF, "true") // Enable EOF detection
        .set(AUTO_OFFSET_RESET, "earliest") // Start from beginning
        .set(ENABLE_AUTO_COMMIT, "false") // Don't auto-commit
        .set(SESSION_TIMEOUT_MS, "6000")
        .set_log_level(RDKafkaLogLevel::Debug);

    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .context("Consumer creation failed")?;

    let topics = &[topic];
    consumer
        .subscribe(topics)
        .map_err(|e| anyhow!("Failed to subscribe to topic: {}", e))?;

    let mut current_message: Option<BorrowedMessage> = None;
    let mut archived_message: Option<serde_json::Value> = None;
    let mut table_data: Vec<DlqMessage> = vec![];
    loop {
        match consumer.recv().await {
            Err(e) => {
                if let rdkafka::error::KafkaError::PartitionEOF(_) = e {
                    info!("Reached end of partition (EOF)");
                    break;
                } else {
                    return Err(anyhow!("Kafka error while reading message: {}", e));
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

                let item = DlqMessage::parse(json.clone());
                if item.id.eq(id) {
                    table_data.push(item);
                    archived_message = Some(json);
                    current_message = Some(m);
                    break;
                }
            }
        };
    }

    let mut archive_message = archived_message.with_context(|| "message not found")?;
    let current_message = current_message.with_context(|| "message not found")?;

    let mut table = Table::new(table_data);
    table.with(Rotate::Left);
    println!("{table}");

    println!("Archiving..");

    let producer: &FutureProducer = &ClientConfig::new()
        .set(BOOTSTRAP_SERVERS, brokers)
        .set(MESSAGE_TIMEOUT_MS, TIMEOUT_MS)
        .create()
        .context("Producer creation error")?;

    archive_message[FIELD_METADATA][METADATA_ARCHIVED_AT] = Value::String(chrono::Utc::now().to_rfc3339());
    let payload: Vec<u8> = serde_json::to_vec(&archive_message)
        .context("Failed to serialize the message")?;

    producer
        .send(
            FutureRecord::to(archive_topic)
                .payload(&payload)
                .key(archive_message[FIELD_ID].as_str().unwrap_or("-")),
            Duration::from_secs(0),
        )
        .await
        .map_err(|e| anyhow!("Failed to archive the message: {:?}", e))?;

    println!("Archived to {}", archive_topic);

    // commit current message
    consumer.commit_message(&current_message, CommitMode::Sync)?;

    Ok(())
}
