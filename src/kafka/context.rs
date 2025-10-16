use rdkafka::{consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer}, error::KafkaResult, ClientContext, TopicPartitionList};

pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, _rebalance: &Rebalance) {
        // TODO: Add custom logic here if needed
        // info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, _rebalance: &Rebalance) {
        // TODO: Add custom logic here if needed
        // info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, _result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        // TODO: Add custom logic here if needed
        // info!("Committing offsets: {:?}", result);
    }
}

// A type alias with custom consumer can be created for convenience.
pub type LoggingConsumer = StreamConsumer<CustomContext>;
