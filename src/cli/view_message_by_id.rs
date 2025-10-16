use log::{info, warn};
use rdkafka::{config::RDKafkaLogLevel, consumer::Consumer, ClientConfig, Message};
use serde_json::from_str;
use tabled::{settings::Rotate, Table};

use crate::{cli::DlqMessage, kafka::{CustomContext, LoggingConsumer}};

pub async fn view_message_by_id(
    brokers: &str,
    group_id: &str,
    topic: &str,
    id: &str,
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
                if item.id.eq(id) || item.correlation_id.eq(id) {
                    table_data.push(item);
                    break;
                }
            }
        };
    }

    let mut table = Table::new(table_data);
    table.with(Rotate::Left);
    println!("{table}");
    Ok(())
}

