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
    /// Archive a message.
    ArchiveMessage {
        topic: String,
        message_id: String,
    },
    /// Republish a DLQ message to its original topic.
    /// 
    /// Finds a message in the DLQ by ID, optionally replaces its payload with a fixed
    /// version from a JSON file, shows a diff preview, then publishes to the original
    /// topic and removes the message from the DLQ.
    RepublishMessage {
        /// DLQ topic to read from
        topic: String,
        /// Message ID or correlation ID to find
        message_id: String,
        /// Path to JSON file with fixed payload (optional)
        /// If not provided, republishes the original payload as-is
        #[arg(long, value_name = "FILE", help = "Path to fixed payload file")]
        payload_file: Option<std::path::PathBuf>,
        /// Preview changes without publishing (dry run) (default: false)
        #[arg(long, default_value = "false")]
        dry_run: bool,
    },
}