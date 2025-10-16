use std::time::Duration;

use anyhow::anyhow;
use log::{info, warn};
use rdkafka::{config::RDKafkaLogLevel, consumer::{CommitMode, Consumer}, message::BorrowedMessage, producer::{FutureProducer, FutureRecord}, ClientConfig, Message};
use serde_json::{from_str, Value};
use tabled::{settings::Rotate, Table};

use crate::{DlqMessage, kafka::{CustomContext, LoggingConsumer}};


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

    let mut current_message: Option<BorrowedMessage> = None;
    let mut archived_message: Option<serde_json::Value> = None;
    let mut table_data:Vec<DlqMessage> = vec!();
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
                let item = DlqMessage::parse(json);
                if item.id.eq(id) {
                    table_data.push(item);
                    archived_message = from_str(payload).expect("Failed to parse");
                    current_message = Some(m);
                    break;
                }
            }
        };
    }

    if archived_message.is_none() || current_message.is_none() {
        return Err(anyhow!("message not found"));
    }
    let mut table = Table::new(table_data);
    table.with(Rotate::Left);
    println!("{table}");

    let mut archive_message = archived_message.unwrap();
    let current_message = current_message.unwrap();

    println!("Archiving..");

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    archive_message["metadata"]["archivedAt"] = Value::String(chrono::Utc::now().to_rfc3339());
    let payload: Vec<u8> = serde_json::to_vec(&archive_message)?;
    
    producer.send(
        FutureRecord::to(archive_topic)
            .payload(&payload)
            .key(archive_message["id"].as_str().unwrap_or("-")),
        Duration::from_secs(0),
    ).await.expect("Failed to archive the message");
    
    println!("Archived to {}", archive_topic);
    
    // commit current message
    consumer.commit_message(&current_message, CommitMode::Sync)?;

    Ok(())
}