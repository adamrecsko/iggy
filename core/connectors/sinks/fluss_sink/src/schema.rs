use fluss::{
    error::Error,
    metadata::{DataTypes, Schema, TableDescriptor},
    row::GenericRow,
};
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, TopicMetadata};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct MetadataEnvelope {
    metadata: IggyMetadata,
    payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct IggyMetadata {
    iggy_id: String,
    iggy_offset: u64,
    iggy_timestamp: u64,
    iggy_stream: String,
    iggy_topic: String,
    iggy_partition_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    iggy_checksum: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    iggy_origin_timestamp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    iggy_headers: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Default)]
pub struct IggyDefaultTable {
    include_checksum: bool,
    include_timestamp: bool,
    include_origin_timestamp: bool,
    include_metadata: bool,
    primary_key: Vec<String>,
}

impl IggyDefaultTable {
    pub fn create_schema(&self) -> Result<Schema, Error> {
        let mut schema_builder = Schema::builder().column("id", DataTypes::string());

        if !self.primary_key.is_empty() {
            schema_builder = schema_builder.primary_key(self.primary_key.clone());
        }

        if self.include_checksum {
            schema_builder = schema_builder.column("checksum", DataTypes::string());
        }
        if self.include_timestamp {
            schema_builder = schema_builder.column("timestamp", DataTypes::string());
        }
        if self.include_metadata {
            schema_builder = schema_builder.column("iggy_offset", DataTypes::string());
            schema_builder = schema_builder.column("iggy_timestamp", DataTypes::string());
            schema_builder = schema_builder.column("iggy_stream", DataTypes::string());
            schema_builder = schema_builder.column("iggy_topic", DataTypes::string());
            schema_builder = schema_builder.column("iggy_partition_id", DataTypes::bigint());
        }

        schema_builder = schema_builder.column("payload", DataTypes::bytes());

        schema_builder.build()
    }

    pub fn create_table_descriptor(&self) -> Result<TableDescriptor, Error> {
        let schema = self.create_schema()?;
        TableDescriptor::builder().schema(schema).build()
    }

    pub fn create_generic_row(
        &self,
        schema: &Schema,
        message: &ConsumedMessage,
        metadata: &MessagesMetadata,
        topic_metadata: &TopicMetadata,
    ) -> Result<GenericRow<'_>, Error> {
        let mut row = GenericRow::new(schema.columns().len());
        let mut idx = 0;
        row.set_field(idx, message.id.to_string());
        idx += 1;

        if self.include_checksum {
            row.set_field(idx, message.checksum.to_string());
            idx += 1;
        }
        if self.include_timestamp {
            row.set_field(idx, message.timestamp.to_string());
            idx += 1;
        }

        if self.include_metadata {
            row.set_field(idx, message.offset.to_string());
            idx += 1;

            row.set_field(idx, message.timestamp.to_string());
            idx += 1;

            row.set_field(idx, topic_metadata.stream.clone());
            idx += 1;

            row.set_field(idx, topic_metadata.topic.clone());
            idx += 1;

            row.set_field(idx, i64::from(metadata.partition_id));
            idx += 1;
        }

        let payload_bytes =
            message
                .payload
                .try_to_bytes()
                .map_err(|error| Error::UnexpectedError {
                    message: "Failed to serialize payload".to_string(),
                    source: Some(Box::new(error)),
                })?;

        row.set_field(idx, payload_bytes);

        Ok(row)
    }
}
