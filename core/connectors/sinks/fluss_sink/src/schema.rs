use fluss::{
    error::Error,
    metadata::{DataType, DataTypes, Schema, TableDescriptor},
    row::{Datum, GenericRow},
};
use iggy_connector_sdk::ConsumedMessage;

use crate::FlussSinkConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    MessageId,
    Checksum,
    MessageTimestamp,
    OriginTimestamp,
    MessageOffset,
    Stream,
    Topic,
    PartitionId,
    BinaryPayload,
    JSONPayload,
}

impl ColumnKind {
    const fn name(self) -> &'static str {
        match self {
            Self::MessageId => "id",
            Self::Checksum => "checksum",
            Self::MessageTimestamp => "iggy_timestamp",
            Self::OriginTimestamp => "iggy_origin_timestamp",
            Self::MessageOffset => "iggy_offset",
            Self::Stream => "iggy_stream",
            Self::Topic => "iggy_topic",
            Self::PartitionId => "iggy_partition_id",
            Self::BinaryPayload => "payload",
            Self::JSONPayload => "payload",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::MessageId
            | Self::Checksum
            | Self::MessageTimestamp
            | Self::OriginTimestamp
            | Self::MessageOffset
            | Self::Stream
            | Self::Topic => DataTypes::string(),
            Self::PartitionId => DataTypes::bigint(),
            Self::BinaryPayload => DataTypes::bytes(),
            Self::JSONPayload => DataTypes::string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RowContext<'a> {
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: u32,
}

impl ColumnKind {
    fn datum<'a>(
        self,
        message: &'a ConsumedMessage,
        context: RowContext<'a>,
    ) -> Result<Datum<'a>, iggy_connector_sdk::Error> {
        match self {
            Self::MessageId => Ok(message.id.to_string().into()),
            Self::Checksum => Ok(message.checksum.to_string().into()),
            Self::MessageTimestamp => Ok(message.timestamp.to_string().into()),
            Self::OriginTimestamp => Ok(message.origin_timestamp.to_string().into()),
            Self::MessageOffset => Ok(message.offset.to_string().into()),
            Self::Stream => Ok(context.stream.into()),
            Self::Topic => Ok(context.topic.into()),
            Self::PartitionId => Ok(i64::from(context.partition_id).into()),
            Self::BinaryPayload => message
                .payload
                .clone()
                .try_into_vec()
                .map(Into::into)
                .map_err(|_| {
                    iggy_connector_sdk::Error::Serialization(
                        "Convert to Fluss Datum has failed".to_string(),
                    )
                }),
            Self::JSONPayload => {
                let payload_bytes = message.payload.clone().try_into_vec()?;
                let payload_string = String::from_utf8(payload_bytes).map_err(|e| {
                    let err_msg = format!("Failed to parse payload as UTF-8 text: {e}");
                    iggy_connector_sdk::Error::Serialization(err_msg)
                })?;
                Ok(payload_string.into())
            }
        }
    }
}

#[derive(Debug)]
pub struct FlussTableLayout {
    columns: Vec<ColumnKind>,
    primary_key_columns: Vec<String>,
}

impl FlussTableLayout {
    pub fn from_config(config: &FlussSinkConfig) -> Self {
        let mut columns: Vec<ColumnKind> = Vec::with_capacity(10);
        columns.push(ColumnKind::MessageId);

        if config.include_checksum {
            columns.push(ColumnKind::Checksum);
        };

        if config.include_metadata {
            columns.extend([
                ColumnKind::MessageOffset,
                ColumnKind::MessageTimestamp,
                ColumnKind::Stream,
                ColumnKind::Topic,
                ColumnKind::PartitionId,
            ]);
        };

        if config.include_origin_timestamp {
            columns.push(ColumnKind::OriginTimestamp);
        }

        match config.payload_format.as_str() {
            "bytea" => columns.push(ColumnKind::BinaryPayload),
            "json" => columns.push(ColumnKind::JSONPayload),
            "text" => columns.push(ColumnKind::JSONPayload),
            _ => panic!("Unsupported payload format: {}", config.payload_format),
        }

        Self {
            columns,
            primary_key_columns: Vec::new(),
        }
    }

    fn build_schema(&self) -> Result<Schema, Error> {
        let mut schema_builder = Schema::builder();
        for column in &self.columns {
            schema_builder = schema_builder.column(column.name(), column.data_type());
        }

        if !self.primary_key_columns.is_empty() {
            schema_builder = schema_builder.primary_key(self.primary_key_columns.clone());
        }

        schema_builder.build()
    }

    pub fn build_table_descriptor(&self) -> Result<TableDescriptor, Error> {
        let schema = self.build_schema()?;
        TableDescriptor::builder().schema(schema).build()
    }

    pub fn row_from_message<'a>(
        &self,
        message: &'a ConsumedMessage,
        context: RowContext<'a>,
    ) -> Result<GenericRow<'a>, iggy_connector_sdk::Error> {
        let mut values: Vec<Datum> = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            values.push(column.datum(message, context)?);
        }
        Ok(GenericRow::from_data(values))
    }
}
