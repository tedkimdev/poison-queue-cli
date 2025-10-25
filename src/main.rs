use clap::Parser;

pub mod cli;
pub mod kafka;

use cli::*;

#[tokio::main]
async fn main() {
    let cli = cli::commands::Args::parse();

    // TODO: Load from config.
    let brokers = "localhost:9092";
    let group_id = "poison_queue_cli_consumer_group_id";
    let archive_topic = "dlq-archive";

    match cli.command {
        Some(Commands::ListTopics) => {
            if let Err(e) = list_topics(brokers).await {
                eprintln!("Error listing topics: {}", e);
                std::process::exit(1);
            }
        },
        Some(Commands::ListMessages {
            topic,
        }) => {
            if let Err(e) = list_messages(brokers, group_id, &topic).await {
                eprintln!("Error listing messages: {}", e);
                std::process::exit(1);
            }
        },
        Some(Commands::ViewMessage {
            topic,
            message_id,
        }) => {
            if let Err(e) = view_message_by_id(brokers, group_id, &topic, &message_id).await {
                eprintln!("Error listing messages: {}", e);
                std::process::exit(1);
            }
        },
        Some(Commands::ArchiveMessage {
            topic,
            message_id,
        }) => {
            if let Err(e) = archive_message(brokers, group_id, &topic, &message_id, archive_topic).await {
                eprintln!("Error archiving message: {}", e);
                std::process::exit(1);
            }
        },
        Some(Commands::RepublishMessage {
            topic,
            message_id,
            payload_file,
            dry_run,
        }) => {
            if let Err(e) = republish_message(
                brokers,
                group_id,
                &topic,
                &message_id,
                payload_file.as_deref(),
                dry_run,
            ).await {
                eprintln!("Error republishing message: {}", e);
                std::process::exit(1);
            }
        },
        None => {
            println!("Run with --help to see instructions");
            std::process::exit(0);
        }
    }
    
}
