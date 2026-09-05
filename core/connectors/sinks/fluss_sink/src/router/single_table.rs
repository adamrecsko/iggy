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

use fluss::metadata::TablePath;
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, TopicMetadata};
use tracing::error;

use super::Error;
use crate::{
    FlussSinkConfig,
    schema_catalog::SchemaCatalog,
    static_schema::{RowContext, SingleTableBatchBuilder, SingleTableConfig},
    writer::{Op, Stat, TableWriter},
};

#[derive(Debug)]
pub(crate) struct SingleTableRouter {
    schema_catalog: SchemaCatalog,
    table_path: TablePath,
    auto_create_table: bool,
    config: SingleTableConfig,
}

impl SingleTableRouter {
    pub(crate) fn new(config: &FlussSinkConfig) -> Self {
        let table_path =
            TablePath::new(config.target_database.clone(), config.target_table.clone());
        let schema_catalog = SchemaCatalog::default();
        Self {
            schema_catalog,
            table_path,
            auto_create_table: config.auto_create_table,
            config: config.into(),
        }
    }

    pub(crate) async fn init(&self, writer: &impl TableWriter) -> Result<(), Error> {
        if self.auto_create_table {
            let entry = self
                .schema_catalog
                .create_and_store_entry_from_table(&self.table_path, writer)
                .await?;

            if entry.is_none() {
                let entry = self
                    .schema_catalog
                    .create_and_store_static_schema_entry(&self.table_path, &self.config)?;

                writer
                    .create_table_if_not_exists(&self.table_path, &entry.table_descriptor)
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn route(
        &self,
        writer: &impl TableWriter,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<Stat, Error> {
        let mut stat = Stat::default();
        let context = RowContext {
            topic: topic_metadata.topic.clone(),
            stream: topic_metadata.stream.clone(),
            partition_id: messages_metadata.partition_id,
        };
        let mut builder = SingleTableBatchBuilder::new(&self.config);
        for message in messages {
            if let Err(error) = builder.append(message) {
                error!(
                    "FlussSink: Can not add record to batch builder, skipping message because of error: [{error}]"
                );
                stat.inc_err();
            } else {
                stat.inc_appended();
            }
        }
        let record_batch = builder.finish(&context);
        writer
            .write_to_table(&self.table_path, Op::Append, record_batch)
            .await?;

        Ok(stat)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use arrow::{
        array::{Int64Array, StringArray},
        record_batch::RecordBatch,
    };
    use fluss::{
        client::FlussTable,
        metadata::{TableDescriptor, TablePath},
    };
    use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, Payload, Schema, TopicMetadata};

    use super::{Error as RouterError, SingleTableRouter};
    use crate::{
        FlussSinkConfig, PayloadFormat,
        static_schema::SingleTableLayout,
        writer::{Op, Stat, TableWriter, WriterError},
    };

    const MESSAGE_TIMESTAMP: u64 = 1_700_000_000_123_456;
    const ORIGIN_TIMESTAMP: u64 = 1_700_000_000_120_789;

    struct TableCreation {
        table_path: TablePath,
        table_descriptor: TableDescriptor,
    }

    struct TableWrite {
        table_path: TablePath,
        batch: RecordBatch,
        op: Op,
    }

    #[derive(Default)]
    struct RecordingWriter {
        table_creations: Mutex<Vec<TableCreation>>,
        table_writes: Mutex<Vec<TableWrite>>,
        fail_table_creation: bool,
        fail_table_write: bool,
    }

    impl TableWriter for RecordingWriter {
        async fn write_to_table(
            &self,
            table_path: &TablePath,
            op: Op,
            batch: RecordBatch,
        ) -> Result<(), WriterError> {
            if self.fail_table_write {
                return Err(WriterError::ConnectionNotInitialized);
            }

            self.table_writes
                .lock()
                .expect("Table writes lock should not be poisoned")
                .push(TableWrite {
                    table_path: table_path.clone(),
                    batch,
                    op,
                });
            Ok(())
        }

        async fn create_table_if_not_exists(
            &self,
            table_path: &TablePath,
            table_descriptor: &TableDescriptor,
        ) -> Result<(), WriterError> {
            if self.fail_table_creation {
                return Err(WriterError::ConnectionNotInitialized);
            }

            self.table_creations
                .lock()
                .expect("Table creations lock should not be poisoned")
                .push(TableCreation {
                    table_path: table_path.clone(),
                    table_descriptor: table_descriptor.clone(),
                });
            Ok(())
        }

        async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>, WriterError> {
            Err(WriterError::TableNotFound {
                table_path: table_path.to_owned(),
            })
        }
    }

    fn test_config() -> FlussSinkConfig {
        FlussSinkConfig {
            target_database: "analytics".to_string(),
            target_table: "events".to_string(),
            payload_format: PayloadFormat::Text,
            ..FlussSinkConfig::default()
        }
    }

    fn test_topic_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "orders".to_string(),
            topic: "created".to_string(),
        }
    }

    fn test_messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 7,
            current_offset: 202,
            schema: Schema::Text,
        }
    }

    fn test_message(id: u128, payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id,
            offset: id as u64,
            checksum: id as u64 + 100,
            timestamp: MESSAGE_TIMESTAMP,
            origin_timestamp: ORIGIN_TIMESTAMP,
            headers: None,
            payload,
        }
    }

    fn run_async<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Runtime::new()
            .expect("Tokio runtime should build")
            .block_on(future)
    }

    fn assert_stats(
        stats: &Stat,
        expected_messages_processed: u64,
        expected_insertion_errors: u64,
    ) {
        assert_eq!(stats.appended, expected_messages_processed);
        assert_eq!(stats.errors, expected_insertion_errors);
    }

    #[test]
    fn given_target_config_when_creating_router_should_preserve_routing_settings() {
        let config = FlussSinkConfig {
            auto_create_table: false,
            ..test_config()
        };

        let router = SingleTableRouter::new(&config);

        assert_eq!(router.table_path, TablePath::new("analytics", "events"));
        assert!(!router.auto_create_table);
    }

    #[test]
    fn given_auto_create_enabled_when_initializing_should_create_expected_table() {
        let config = test_config();
        let router = SingleTableRouter::new(&config);
        let writer = RecordingWriter::default();

        run_async(router.init(&writer)).expect("Router should initialize");

        let table_creations = writer
            .table_creations
            .lock()
            .expect("Table creations lock should not be poisoned");
        assert_eq!(table_creations.len(), 1);
        assert_eq!(table_creations[0].table_path, router.table_path);
        assert_eq!(
            table_creations[0].table_descriptor,
            SingleTableLayout::from_single_table_config(&router.config)
                .build_table_descriptor()
                .expect("Expected table descriptor should build")
        );
    }

    #[test]
    fn given_auto_create_disabled_when_initializing_should_not_create_table() {
        let config = FlussSinkConfig {
            auto_create_table: false,
            ..test_config()
        };
        let router = SingleTableRouter::new(&config);
        let writer = RecordingWriter::default();

        run_async(router.init(&writer)).expect("Router should initialize");

        assert!(
            writer
                .table_creations
                .lock()
                .expect("Table creations lock should not be poisoned")
                .is_empty()
        );
    }

    #[test]
    fn given_table_creation_failure_when_initializing_should_return_writer_error() {
        let router = SingleTableRouter::new(&test_config());
        let writer = RecordingWriter {
            fail_table_creation: true,
            ..RecordingWriter::default()
        };

        let error = run_async(router.init(&writer)).expect_err("Table creation should fail");

        assert!(matches!(
            error,
            RouterError::WriterError(WriterError::ConnectionNotInitialized)
        ));
    }

    #[test]
    fn given_valid_messages_when_routing_should_write_batch_with_context_and_return_success_stats()
    {
        let router = SingleTableRouter::new(&test_config());
        let writer = RecordingWriter::default();
        let messages = vec![
            test_message(101, Payload::Text("first".to_string())),
            test_message(102, Payload::Text("second".to_string())),
        ];

        let stats = run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            messages,
        ))
        .expect("Messages should be routed");

        assert_stats(&stats, 2, 0);
        let table_writes = writer
            .table_writes
            .lock()
            .expect("Table writes lock should not be poisoned");
        assert_eq!(table_writes.len(), 1);
        assert_eq!(table_writes[0].table_path, router.table_path);
        assert_eq!(table_writes[0].batch.num_rows(), 2);

        let streams = table_writes[0]
            .batch
            .column_by_name("iggy_stream")
            .expect("Stream column should exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Stream column should contain strings");
        let topics = table_writes[0]
            .batch
            .column_by_name("iggy_topic")
            .expect("Topic column should exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Topic column should contain strings");
        let partition_ids = table_writes[0]
            .batch
            .column_by_name("iggy_partition_id")
            .expect("Partition ID column should exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Partition ID column should contain integers");

        assert_eq!(streams.iter().collect::<Vec<_>>(), vec![Some("orders"); 2]);
        assert_eq!(topics.iter().collect::<Vec<_>>(), vec![Some("created"); 2]);
        assert_eq!(partition_ids.iter().collect::<Vec<_>>(), vec![Some(7); 2]);
    }

    #[test]
    fn given_invalid_and_valid_messages_when_routing_should_skip_invalid_message() {
        let router = SingleTableRouter::new(&test_config());
        let writer = RecordingWriter::default();
        let messages = vec![
            test_message(101, Payload::Raw(vec![0xff])),
            test_message(102, Payload::Text("valid".to_string())),
        ];

        let stats = run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            messages,
        ))
        .expect("Valid message should be routed");

        assert_stats(&stats, 1, 1);
        let table_writes = writer
            .table_writes
            .lock()
            .expect("Table writes lock should not be poisoned");
        assert_eq!(table_writes.len(), 1);
        assert_eq!(table_writes[0].batch.num_rows(), 1);
        let payloads = table_writes[0]
            .batch
            .column_by_name("payload")
            .expect("Payload column should exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Payload column should contain strings");
        assert_eq!(payloads.value(0), "valid");
    }

    #[test]
    fn given_write_messages_router_should_write_with_append_mode() {
        let router = SingleTableRouter::new(&test_config());
        let writer = RecordingWriter::default();
        let messages = vec![test_message(102, Payload::Text("valid".to_string()))];

        run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            messages,
        ))
        .expect("Valid message should be routed");

        let table_writes = writer
            .table_writes
            .lock()
            .expect("Table writes lock should not be poisoned");
        assert_eq!(table_writes.len(), 1);

        let op = &table_writes[0].op;
        assert_eq!(op, &Op::Append);
    }

    #[test]
    fn given_table_write_failure_when_routing_should_return_writer_error() {
        let router = SingleTableRouter::new(&test_config());
        let writer = RecordingWriter {
            fail_table_write: true,
            ..RecordingWriter::default()
        };

        let error = run_async(router.route(
            &writer,
            &test_topic_metadata(),
            test_messages_metadata(),
            vec![test_message(101, Payload::Text("payload".to_string()))],
        ))
        .err()
        .expect("Table write should fail");

        assert!(matches!(error, RouterError::WriterError { .. }));
    }
}
