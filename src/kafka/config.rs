// Kafka configuration key constants
pub const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
pub const GROUP_ID: &str = "group.id";
pub const ENABLE_PARTITION_EOF: &str = "enable.partition.eof";
pub const AUTO_OFFSET_RESET: &str = "auto.offset.reset";
pub const ENABLE_AUTO_COMMIT: &str = "enable.auto.commit";
pub const SESSION_TIMEOUT_MS: &str = "session.timeout.ms";
pub const MESSAGE_TIMEOUT_MS: &str = "message.timeout.ms";

// Timeout value (in milliseconds)
pub const TIMEOUT_MS: &str = "6000";  // 6 seconds

// Kafka message headers
pub const MESSAGE_HEADER_MESSAGE_ID: &str = "id";
pub const MESSAGE_HEADER_CORRELATION_ID: &str = "correlation_id";
pub const MESSAGE_HEADER_REPUBLISHED_AT: &str = "republished_at";