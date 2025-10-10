use clap::Parser;

pub mod cli;
pub mod models;
pub mod kafka;

use cli::*;

#[tokio::main]
async fn main() {
    let cli = cli::commands::Args::parse();

    // TODO: Load from config.
    let brokers = "localhost:9092";

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
            todo!();
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
