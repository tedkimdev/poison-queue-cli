use tabled::Tabled;

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
        let id = json["id"].as_str().unwrap_or("-");
        let correlation_id = json["correlationId"].as_str().unwrap_or("-");
        let reason = json["metadata"]["failureReason"].as_str().unwrap_or("-");
        let retries = json["metadata"]["retryCount"].as_i64().unwrap_or(0);
        let original = json["metadata"]["originalTopic"].as_str().unwrap_or("-");
        let moved_at = json["metadata"]["movedToDlqAt"].as_str().unwrap_or("-");

        let metadata = serde_json::to_string_pretty(&json["metadata"])
            .unwrap_or_else(|_| "-".to_string());
        let payload = serde_json::to_string_pretty(&json["payload"])
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