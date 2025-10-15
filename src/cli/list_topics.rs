use std::time::Duration;

use anyhow::{anyhow};
use rdkafka::{consumer::Consumer, ClientConfig};

use crate::kafka::{CustomContext, LoggingConsumer};

const DEAD_LETTER_QUEUE_PREFIX: &str = "dlq-";

pub async fn list_topics(brokers: &str,) -> Result<(), anyhow::Error> {
    let context = CustomContext;

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", "topic-lister")
        .set("session.timeout.ms", "6000");
    
    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .expect("Consumer creation failed");
    
    // Fetch metadata for all topics (pass None to get all topics)
    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .map_err(|e| anyhow!("Failed to fetch Kafka metadata: {:?}", e))
        .expect("Unable to fetch Kafka metadata");

    if metadata.topics().is_empty() {
        println!("🔍 No topics found in Kafka cluster");
        return Ok(());
    }
    
    // Organize topics by type
    let mut regular_topics = Vec::new();
    let mut dlq_topics = Vec::new();
    let mut internal_topics = Vec::new();
    
    for topic in metadata.topics() {
        let topic_name = topic.name();
        let partitions = topic.partitions().len();
        let replication_factor = topic.partitions()
            .first()
            .map(|p| p.replicas().len())
            .unwrap_or(0);
        
        // Kafka's rdkafka does not expose is_internal; use naming convention if needed
        let is_internal = topic_name.starts_with("__");

        let topic_info = TopicDisplayInfo {
            name: topic_name.to_string(),
            partitions,
            replication_factor,
            is_internal,
        };

        if is_internal {
            internal_topics.push(topic_info);
        } else if topic_name.starts_with(DEAD_LETTER_QUEUE_PREFIX) {
            dlq_topics.push(topic_info);
        } else {
            regular_topics.push(topic_info);
        }
    }
    
    // Sort each category
    regular_topics.sort_by(|a, b| a.name.cmp(&b.name));
    dlq_topics.sort_by(|a, b| a.name.cmp(&b.name));
    internal_topics.sort_by(|a, b| a.name.cmp(&b.name));
    
    // Print header
    println!("📋 Kafka Topics Overview");
    println!("{}", "═".repeat(70));
    println!("🔗 Broker: {}", brokers);
    println!("📊 Total Topics: {}", metadata.topics().len());
    println!();
    
    // Print regular topics
    if !regular_topics.is_empty() {
        println!("📋 Regular Topics ({})", regular_topics.len());
        println!("{}", "─".repeat(70));
        print_topic_table(&regular_topics, "🟢");
        println!();
    }
    
    // Print DLQ topics
    if !dlq_topics.is_empty() {
        println!("💀 Dead Letter Queue Topics ({})", dlq_topics.len());
        println!("{}", "─".repeat(70));
        print_topic_table(&dlq_topics, "🔴");
        println!();
    }
    
    // Print internal topics (optional - you might want to hide these)
    if !internal_topics.is_empty() {
        println!("🔧 Internal Topics ({})", internal_topics.len());
        println!("{}", "─".repeat(70));
        print_topic_table(&internal_topics, "⚙️");
        println!();
    }
    
    // Print summary
    println!("{}", "═".repeat(70));
    println!("📈 Summary:");
    println!("   • Regular Topics: {}", regular_topics.len());
    println!("   • DLQ Topics: {}", dlq_topics.len());
    println!("   • Internal Topics: {}", internal_topics.len());
    println!("   • Total: {}", metadata.topics().len());
    
    Ok(())
}

#[derive(Debug)]
struct TopicDisplayInfo {
    name: String,
    partitions: usize,
    replication_factor: usize,
    is_internal: bool,
}

fn print_topic_table(topics: &[TopicDisplayInfo], icon: &str) {
    // Print table header
    println!("{:<35} {:<12} {:<8} {:<10}", "TOPIC NAME", "TYPE", "PARTS", "REPLICATION");
    println!("{}", "─".repeat(70));
    
    // Print each topic
    for topic in topics {
        let topic_type = if topic.is_internal {
            "Internal"
        } else if topic.name.starts_with("dlq-") || topic.name.contains("-dlq") {
            "DLQ"
        } else {
            "Regular"
        };
        
        println!("{:<35} {:<12} {:<8} {:<10}", 
            format!("{} {}", icon, topic.name),
            topic_type,
            topic.partitions,
            topic.replication_factor
        );
    }
}