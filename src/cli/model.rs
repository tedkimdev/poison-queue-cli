use tabled::Tabled;

// DLQ message field names
pub const FIELD_ID: &str = "id";
pub const FIELD_CORRELATION_ID: &str = "correlationId";
pub const FIELD_PAYLOAD: &str = "payload";
pub const FIELD_METADATA: &str = "metadata";

// Metadata field names
pub const METADATA_ARCHIVED_AT: &str = "archivedAt";
pub const METADATA_FAILURE_REASON: &str = "failureReason";
pub const METADATA_RETRY_COUNT: &str = "retryCount";
pub const METADATA_ORIGINAL_TOPIC: &str = "originalTopic";
pub const METADATA_MOVED_TO_DLQ_AT: &str = "movedToDlqAt";

#[derive(Tabled)]
pub struct DlqMessage {
    pub payload: String,
    pub metadata: String,
    pub moved_at: String,
    pub original_topic: String,
    pub retries: i64,
    pub reason: String,
    pub correlation_id: String,
    pub id: String,
}

impl DlqMessage {
    pub fn parse(json: serde_json::Value) -> Self {
        let id = json[FIELD_ID].as_str().unwrap_or("-");
        let correlation_id = json[FIELD_CORRELATION_ID].as_str().unwrap_or("-");
        let reason = json[FIELD_METADATA][METADATA_FAILURE_REASON].as_str().unwrap_or("-");
        let retries = json[FIELD_METADATA][METADATA_RETRY_COUNT].as_i64().unwrap_or(0);
        let original = json[FIELD_METADATA][METADATA_ORIGINAL_TOPIC].as_str().unwrap_or("-");
        let moved_at = json[FIELD_METADATA][METADATA_MOVED_TO_DLQ_AT].as_str().unwrap_or("-");

        let metadata = serde_json::to_string_pretty(&json[FIELD_METADATA])
            .unwrap_or_else(|_| "-".to_string());
        let payload = serde_json::to_string_pretty(&json[FIELD_PAYLOAD])
            .unwrap_or_else(|_| "-".to_string());
        
        Self {
            id: id.to_string(),
            correlation_id: correlation_id.to_string(),
            reason: reason.to_string(),
            retries,
            original_topic: original.to_string(),
            moved_at: moved_at.to_string(),
            metadata: metadata.to_string(),
            payload: payload.to_string(),
        }
    }
}