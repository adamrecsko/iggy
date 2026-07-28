use async_trait::async_trait;
use fluss::metadata::TablePath;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::info;

use crate::{schema::FlussTableLayout, writer::FlussWriter};

mod schema;
mod writer;

sink_connector!(FlussSink);

#[derive(Debug)]
struct State {
    invocations_count: usize,
}

#[derive(Debug)]
pub struct FlussSink {
    id: u32,
    state: Mutex<State>,
    fluss_writer: writer::FlussWriter,
    fluss_config: FlussSinkConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FlussSinkConfig {
    pub bootstrap_servers: String,
    pub fluss_database: String,
    pub fluss_table: String,
    pub batch_size: u32,
    pub auto_create_table: bool,
    pub include_metadata: bool,
    pub include_checksum: bool,
    pub include_origin_timestamp: bool,
    pub payload_format: String,
    pub create_table: bool,
}

impl FlussSink {
    #[allow(dead_code)]
    fn new(id: u32, config: FlussSinkConfig) -> Self {
        Self {
            id,
            state: Mutex::new(State {
                invocations_count: 0,
            }),
            fluss_writer: FlussWriter::new(config.clone()),
            fluss_config: config,
        }
    }
}

#[async_trait]
impl Sink for FlussSink {
    async fn open(&mut self) -> Result<(), Error> {
        self.fluss_writer.connect().await
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let invocation = {
            let mut state = self.state.lock().await;
            state.invocations_count += 1;
            state.invocations_count
        };

        info!(
            "Fluss sink with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset,
            invocation
        );
        let table_path = TablePath::new(
            self.fluss_config.fluss_database.clone(),
            self.fluss_config.fluss_table.clone(),
        );

        let table_layout = FlussTableLayout::from_config(&self.fluss_config);

        self.fluss_writer
            .write_to_table(
                table_path,
                messages_metadata,
                messages,
                topic_metadata,
                table_layout,
            )
            .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        // TODO: graceful shutdown fluss client
        info!("Closing Fluss Sink");
        Ok(())
    }
}
