use clap::Parser;

pub mod cli;
use cli::*;

#[tokio::main]
async fn main() {
    println!("Poison Queue CLI!");

    let cli = cli::commands::Args::parse();

    match cli.command {
        Some(Commands::ListTopics) => {
            todo!();
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
