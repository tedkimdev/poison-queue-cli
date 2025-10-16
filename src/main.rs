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
            message_id,
        }) => {
            todo!();
        },
        Some(Commands::EditMessage {
            message_id,
        }) => {
            todo!();
        },
        Some(Commands::ArchiveMessage {
            message_id,
        }) => {
            todo!();
        },
        Some(Commands::RepublishMessage {
            message_id,
        }) => {
            todo!();
        },
        None => {
            println!("Run with --help to see instructions");
            std::process::exit(0);
        }
    }
    
}
