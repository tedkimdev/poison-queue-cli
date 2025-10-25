use std::{
    io::{self, Write},
    path::Path,
    time::Duration,
};

use anyhow::{anyhow, Context};
use colored::Colorize;
use log::{info, warn};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer},
    message::{BorrowedMessage, Headers, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use serde_json::from_str;
use similar::{ChangeTag, TextDiff};

use crate::{
    cli::DlqMessage,
    kafka::{
        CustomContext, LoggingConsumer, AUTO_OFFSET_RESET, BOOTSTRAP_SERVERS, ENABLE_AUTO_COMMIT,
        ENABLE_PARTITION_EOF, GROUP_ID, MESSAGE_HEADER_CORRELATION_ID, MESSAGE_HEADER_MESSAGE_ID,
        MESSAGE_HEADER_REPUBLISHED_AT, MESSAGE_TIMEOUT_MS, SESSION_TIMEOUT_MS, TIMEOUT_MS,
    },
};

pub async fn republish_message(
    brokers: &str,
    group_id: &str,
    dlq_topic: &str,
    message_id: &str,
    payload_file: Option<&Path>,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    // find message in dlq
    let context = CustomContext;
    let mut config = ClientConfig::new();

    config
        .set(GROUP_ID, group_id)
        .set(BOOTSTRAP_SERVERS, brokers)
        .set(ENABLE_PARTITION_EOF, "true") // Enable EOF detection
        .set(AUTO_OFFSET_RESET, "earliest") // Start from beginning
        .set(ENABLE_AUTO_COMMIT, "false") // Don't auto-commit
        .set(SESSION_TIMEOUT_MS, TIMEOUT_MS)
        .set_log_level(RDKafkaLogLevel::Debug);

    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .context("Consumer creation failed")?;

    let topics = &[dlq_topic];
    consumer
        .subscribe(topics)
        .map_err(|e| anyhow!("Failed to subscribe to topic: {}", e))?;

    let mut dlq_message: Option<DlqMessage> = None;
    let mut borrowed_message: Option<BorrowedMessage> = None;
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
                    None => {
                        warn!("No payload found in message, skipping");
                        continue;
                    }
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
                if item.id.eq(message_id) || item.correlation_id.eq(message_id) {
                    dlq_message = Some(item);
                    borrowed_message = Some(m);
                    break;
                }
            }
        };
    }

    let dlq_message = dlq_message.with_context(|| "Message not found")?;
    let original_topic = dlq_message.original_topic.as_str();
    let mut message_json =
        serde_json::to_value(&dlq_message).context("Failed to convert DLQ message to JSON")?;

    let mut headers = OwnedHeaders::new();
    headers = headers
        .insert(rdkafka::message::Header {
            key: MESSAGE_HEADER_MESSAGE_ID,
            value: Some(dlq_message.id.as_bytes()),
        })
        .insert(rdkafka::message::Header {
            key: MESSAGE_HEADER_REPUBLISHED_AT,
            value: Some(chrono::Utc::now().to_rfc3339().as_bytes()),
        });

    if !dlq_message.correlation_id.is_empty() {
        headers = headers.insert(rdkafka::message::Header {
            key: MESSAGE_HEADER_CORRELATION_ID,
            value: Some(dlq_message.correlation_id.as_bytes()),
        });
    }
    
    // handle payload replacement if file is provided
    let mut new_payload: Option<String> = None;
    if let Some(file_path) = payload_file {
        let payload = read_payload_from_file(file_path)?;
        let payload = serde_json::to_string_pretty(&payload)
            .context("Failed to convert new payload to JSON")?;

        new_payload = Some(payload.clone());

        display_diff_and_plan(
            &dlq_message,
            &dlq_message.payload.to_string(),
            &payload,
            dlq_topic,
            file_path,
            dry_run,
            &headers,
        )?;
    } else {
        display_republish_info(&dlq_message, dlq_topic, dry_run, &headers)?;
    }

    if dry_run {
        print_dry_run_info(dlq_topic, original_topic);
        return Ok(());
    }

    if !confirm_action()? {
        println!("Operation cancelled.");
        return Ok(());
    }

    // publish to original topic
    println!("\nPublishing to {}...", original_topic);
    message_json["payload"] =
        serde_json::to_value(new_payload).context("Failed to convert new payload to JSON")?;

    publish_message(brokers, original_topic, &message_json, message_id, headers).await?;
    println!("✅ Message published successfully");

    // commit DLQ offset
    if let Some(borrowed_message) = borrowed_message {
        consumer
            .commit_message(&borrowed_message, CommitMode::Sync)
            .context("Failed to commit DLQ offset")?;
        println!("✅ DLQ message committed (removed from {})", dlq_topic);
    }

    println!("\nDone!");
    Ok(())
}

/// Read and parse payload from JSON file
fn read_payload_from_file(path: &Path) -> Result<serde_json::Value, anyhow::Error> {
    let content = std::fs::read_to_string(path)
        .context(format!("Failed to read file: {}", path.display()))?;

    let payload: serde_json::Value = serde_json::from_str(&content)
        .context(format!("Invalid JSON in file: {}", path.display()))?;

    Ok(payload)
}

/// Display diff when payload is being changed
fn display_diff_and_plan(
    dlq_message: &DlqMessage,
    old_payload: &String,
    new_payload: &String,
    dlq_topic: &str,
    payload_file: &Path,
    dry_run: bool,
    headers: &OwnedHeaders,
) -> Result<(), anyhow::Error> {
    let original_topic = dlq_message.original_topic.as_str();

    println!("\n{}", "=".repeat(65));
    println!("Found message in DLQ: {}", dlq_topic.cyan());
    if dry_run {
        println!("{}", "[DRY RUN MODE]".yellow().bold());
    }
    println!("{}", "=".repeat(65));

    println!("\n{}", "MESSAGE METADATA:".bold());
    println!("  ID:             {}", dlq_message.id);
    println!("  Correlation ID: {}", dlq_message.correlation_id);
    println!("  Failure Reason: {}", dlq_message.reason);
    println!("  Retry Count:    {}", dlq_message.retries);
    println!("  Original Topic: {}", dlq_message.original_topic.green());
    println!("  Moved to DLQ:   {}", dlq_message.moved_at);

    println!("\n{}", "MESSAGE HEADERS (will be republished as-is):".bold().yellow());
    for header in headers.iter() {
        println!("  {}: {}", header.key, String::from_utf8_lossy(header.value.unwrap_or_default()));
    }

    println!("\n{}", "=".repeat(65));
    println!("{}", "PAYLOAD COMPARISON".bold());
    println!("{}", "=".repeat(65));

    println!("\n{}", "BEFORE (Current in DLQ):".yellow());
    println!("{}", old_payload);

    println!(
        "\n{}",
        format!("AFTER (From file: {}):", payload_file.display()).green()
    );
    println!("{}", new_payload);

    println!("\n{}", "DIFF:".cyan());
    print_diff(old_payload, new_payload);

    println!("\n{}", "=".repeat(65));
    println!("{}", "PLANNED ACTION:".bold());
    if dry_run {
        println!(
            "  {} {} Publish fixed message to: {}",
            "[DRY RUN]".yellow(),
            "→".green(),
            original_topic.green()
        );
        println!(
            "  {} {} Remove from DLQ: {}",
            "[DRY RUN]".yellow(),
            "→".yellow(),
            dlq_topic.yellow()
        );
    } else {
        println!(
            "  {} Publish fixed message to: {}",
            "→".green(),
            original_topic.green()
        );
        println!(
            "  {} Remove from DLQ: {} (commit offset)",
            "→".yellow(),
            dlq_topic.yellow()
        );
    }
    println!("{}", "=".repeat(65));

    Ok(())
}

/// Display info when republishing without changes
fn display_republish_info(
    dlq_message: &DlqMessage,
    dlq_topic: &str,
    dry_run: bool,
    headers: &OwnedHeaders,
) -> Result<(), anyhow::Error> {
    let original_topic = dlq_message.original_topic.as_str();

    println!("\n{}", "=".repeat(65));
    println!("Found message in DLQ: {}", dlq_topic.cyan());
    if dry_run {
        println!("{}", "[DRY RUN MODE]".yellow().bold());
    }
    println!("{}", "=".repeat(65));

    println!("\n{}", "MESSAGE METADATA:".bold());
    println!("  ID:             {}", dlq_message.id);
    println!("  Correlation ID: {}", dlq_message.correlation_id);
    println!("  Failure Reason: {}", dlq_message.reason);
    println!("  Retry Count:    {}", dlq_message.retries);
    println!("  Original Topic: {}", dlq_message.original_topic.green());
    println!("  Moved to DLQ:   {}", dlq_message.moved_at);

    println!("\n{}", "MESSAGE HEADERS (will be republished as-is):".yellow());
    for header in headers.iter() {
        println!("  {}: {}", header.key, String::from_utf8_lossy(header.value.unwrap_or_default()));
    }

    println!("\n{}", "PAYLOAD (will be republished as-is):".yellow());
    println!("{}", dlq_message.payload);

    println!("\n{}", "=".repeat(65));
    println!("{}", "PLANNED ACTION:".bold());
    if dry_run {
        println!(
            "  {} {} Republish message to: {}",
            "[DRY RUN]".yellow(),
            "→".green(),
            original_topic.green()
        );
        println!(
            "  {} {} Remove from DLQ: {}",
            "[DRY RUN]".yellow(),
            "→".yellow(),
            dlq_topic.yellow()
        );
    } else {
        println!(
            "  {} Republish message to: {}",
            "→".green(),
            original_topic.green()
        );
        println!("  {} Remove from DLQ: {}", "→".yellow(), dlq_topic.yellow());
    }
    println!("{}", "=".repeat(65));

    Ok(())
}

fn print_diff(old: &str, new: &str) {
    let diff = TextDiff::from_lines(old, new);
    for change in diff.iter_all_changes() {
        let (sign, line) = match change.tag() {
            ChangeTag::Delete => ("-", format!("{}", change).red()),
            ChangeTag::Insert => ("+", format!("{}", change).green()),
            ChangeTag::Equal => (" ", format!("{}", change).normal()),
        };
        print!("{} {}", sign, line);
    }
}

fn print_dry_run_info(dlq_topic: &str, original_topic: &str) {
    println!("\n{}", "=".repeat(65));
    println!(
        "{}",
        "🔍 DRY RUN MODE - No changes will be made".yellow().bold()
    );
    println!("{}", "=".repeat(65));
    println!("\n{}", "What would happen:".cyan());
    println!(
        "  {} Message would be published to: {}",
        "→".green(),
        original_topic.green()
    );
    println!(
        "  {} DLQ offset would be committed (message removed from {})",
        "→".yellow(),
        dlq_topic.yellow()
    );
    println!(
        "\n{}",
        "To actually perform this operation, run without --dry-run".dimmed()
    );
}

/// Publish message to Kafka topic
async fn publish_message(
    brokers: &str,
    topic: &str,
    message: &serde_json::Value,
    key: &str,
    headers: OwnedHeaders,
) -> Result<(), anyhow::Error> {
    let producer: FutureProducer = ClientConfig::new()
        .set(BOOTSTRAP_SERVERS, brokers)
        .set(MESSAGE_TIMEOUT_MS, TIMEOUT_MS)
        .create()
        .context("Producer creation failed")?;

    let payload = serde_json::to_vec(message).context("Failed to serialize message")?;

    producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload)
                .key(key)
                .headers(headers),
            Duration::from_secs(0),
        )
        .await
        .map_err(|(err, _)| anyhow!("Failed to publish: {}", err))?;

    Ok(())
}

/// Ask user for confirmation
fn confirm_action() -> Result<bool, anyhow::Error> {
    print!("\n{} ", "Proceed with this change? [y/N]:".yellow().bold());
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    Ok(input.trim().eq_ignore_ascii_case("y"))
}
