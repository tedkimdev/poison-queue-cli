use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command()]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// List Topics.
    ListTopics,
    /// List Messages in a topic.
    ListMessages {
        topic: String,
    },
    /// List a message.
    ViewMessage {
        topic: String,
        message_id: String,
    },
    /// Edit a message.
    EditMessage {
        message_id: String,
    },
    /// Archive a message.
    ArchiveMessage {
        topic: String,
        message_id: String,
    },
    /// Republish a message to the original topic.
    RepublishMessage {
        message_id: String,
    },
}