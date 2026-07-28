// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use async_trait::async_trait;
use fluss::metadata::TablePath;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info};

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
    table_layout: Option<FlussTableLayout>,
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
            table_layout: None,
        }
    }
}

#[async_trait]
impl Sink for FlussSink {
    async fn open(&mut self) -> Result<(), Error> {
        let table_layout = FlussTableLayout::from_config(&self.fluss_config)?;
        self.fluss_writer.connect().await?;
        self.table_layout = Some(table_layout);
        info!("Opened Fluss sink connector ID: {}", self.id);
        Ok(())
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

        debug!(
            "Fluss sink connector ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition_id: {}, current_offset: {}, invocation: {}",
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

        let table_layout = self
            .table_layout
            .as_ref()
            .ok_or_else(|| Error::InitError("Fluss table layout is not initialized".to_string()))?;

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
        info!("Closing Fluss sink connector ID: {}", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iggy_connector_sdk::{ConsumedMessage, Error, Payload};

    use super::{FlussSinkConfig, schema::FlussTableLayout};
    use crate::schema::RowContext;

    fn test_config() -> FlussSinkConfig {
        FlussSinkConfig {
            bootstrap_servers: "localhost:9123".to_string(),
            fluss_database: "fluss".to_string(),
            fluss_table: "iggy_messages".to_string(),
            batch_size: 100,
            auto_create_table: true,
            include_metadata: true,
            include_checksum: true,
            include_origin_timestamp: true,
            payload_format: "bytea".to_string(),
            create_table: true,
        }
    }

    fn test_message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset: 2,
            checksum: 3,
            timestamp: 4,
            origin_timestamp: 5,
            headers: None,
            payload,
        }
    }

    #[test]
    fn given_unsupported_payload_format_when_building_layout_should_return_invalid_config_value() {
        let mut config = test_config();
        config.payload_format = "xml".to_string();

        let error = FlussTableLayout::from_config(&config)
            .expect_err("Unsupported payload format should fail");

        assert!(matches!(
            error,
            Error::InvalidConfigValue(message)
                if message.contains("unsupported Fluss payload_format 'xml'")
        ));
    }

    #[test]
    fn given_invalid_utf8_when_building_string_row_should_return_serialization_error() {
        let mut config = test_config();
        config.payload_format = "text".to_string();
        let table_layout =
            FlussTableLayout::from_config(&config).expect("Text payload format should be valid");
        let message = test_message(Payload::Raw(vec![0xFF]));
        let context = RowContext {
            stream: "stream",
            topic: "topic",
            partition_id: 0,
        };

        let error = table_layout
            .row_from_message(&message, context)
            .expect_err("Invalid UTF-8 payload should fail");

        assert!(matches!(
            error,
            Error::Serialization(message)
                if message.contains("not valid UTF-8 for the Fluss STRING column")
        ));
    }
}
