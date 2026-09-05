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

use std::{
    fmt::{self, Display, Formatter},
    time::Duration,
};

use arrow::array::RecordBatch;
use fluss::{
    client::{AppendWriter, FlussConnection, FlussTable},
    error::Error::{self as FlussError},
    metadata::{TableDescriptor, TablePath},
};
use iggy_connector_sdk::Error as ConnectorError;
use thiserror::Error;

use crate::FlussSinkConfig;

#[derive(Debug, Error)]
pub(crate) enum WriterError {
    #[error(transparent)]
    Connector(ConnectorError),
    #[error("Fluss connection is not initialized")]
    ConnectionNotInitialized,
    #[error("Failed to connect to Fluss: {source}")]
    Connect {
        #[source]
        source: Box<FlussError>,
    },
    #[error("Invalid Fluss writer configuration: {source}")]
    InvalidWriterConfig {
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to close Fluss connection: {source}")]
    CloseConnection {
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to get Fluss admin client: {source}")]
    GetAdminClient {
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to create Fluss table '{table_path}': {source}")]
    CreateTable {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to get Fluss table, table not found: '{table_path}'")]
    TableNotFound { table_path: TablePath },
    #[error("Failed to get Fluss table '{table_path}' because of error: {source}")]
    GetTableFailed {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to create appender for Fluss table '{table_path}': {source}")]
    CreateAppender {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to create writer for Fluss table '{table_path}': {source}")]
    CreateWriter {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error("Failed to append Arrow batch to Fluss table '{table_path}': {source}")]
    AppendArrowBatch {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
    #[error(
        "Failed to flush rows to Fluss table, data can be partially stored, append operation can duplicate data '{table_path}': {source}"
    )]
    FlushRows {
        table_path: TablePath,
        #[source]
        source: Box<FlussError>,
    },
}

impl From<ConnectorError> for WriterError {
    fn from(error: ConnectorError) -> Self {
        Self::Connector(error)
    }
}

impl From<WriterError> for ConnectorError {
    fn from(error: WriterError) -> Self {
        let message = error.to_string();
        match error {
            WriterError::Connector(source) => source,
            WriterError::ConnectionNotInitialized | WriterError::Connect { .. } => {
                Self::InitError(message)
            }
            WriterError::InvalidWriterConfig { .. } => Self::InvalidConfigValue(message),
            WriterError::CloseConnection { .. } => Self::Connection(message),
            WriterError::GetAdminClient { .. }
            | WriterError::CreateTable { .. }
            | WriterError::GetTableFailed { .. }
            | WriterError::TableNotFound { .. }
            | WriterError::CreateAppender { .. }
            | WriterError::CreateWriter { .. }
            | WriterError::AppendArrowBatch { .. }
            | WriterError::FlushRows { .. } => Self::CannotStoreData(message),
        }
    }
}

#[derive(Default)]
pub(crate) struct Stat {
    pub(crate) errors: u64,
    pub(crate) appended: u64,
    pub(crate) inserted: u64,
}

impl Stat {
    pub(crate) fn add(self, other: Self) -> Self {
        Self {
            errors: self.errors + other.errors,
            appended: self.appended + other.appended,
            inserted: self.inserted + other.inserted,
        }
    }

    pub(crate) fn inc_err(&mut self) {
        self.errors += 1;
    }

    pub(crate) fn inc_err_by(&mut self, count: u64) {
        self.errors += count;
    }

    pub(crate) fn inc_appended(&mut self) {
        self.appended += 1;
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum Op {
    Append,
}

pub(crate) trait TableWriter {
    async fn write_to_table(
        &self,
        table_path: &TablePath,
        op: Op,
        batch: RecordBatch,
    ) -> Result<(), WriterError>;

    async fn create_table_if_not_exists(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
    ) -> Result<(), WriterError>;

    async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>, WriterError>;
}

pub struct FlussWriter {
    connection: Option<FlussConnection>,
    config: FlussSinkConfig,
}

impl Display for FlussWriter {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "FlussWriter")
    }
}

impl fmt::Debug for FlussWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FlussWriter")
            .finish_non_exhaustive()
    }
}

impl FlussWriter {
    pub fn new(config: FlussSinkConfig) -> Self {
        Self {
            config,
            connection: None,
        }
    }

    pub(crate) async fn connect(&mut self) -> Result<(), WriterError> {
        let config = fluss::config::Config::try_from(&self.config)?;
        let connection =
            FlussConnection::new(config)
                .await
                .map_err(|source| WriterError::Connect {
                    source: Box::new(source),
                })?;
        connection.get_or_create_writer_client().map_err(|source| {
            WriterError::InvalidWriterConfig {
                source: Box::new(source),
            }
        })?;
        self.connection = Some(connection);
        Ok(())
    }

    pub(crate) async fn close(&mut self) -> Result<(), WriterError> {
        self.get_connection()?
            .close(Duration::from_secs(30))
            .await
            .map_err(|source| WriterError::CloseConnection {
                source: Box::new(source),
            })?;
        self.connection = None;
        Ok(())
    }
    async fn append_record_batch(
        &self,
        table_path: &TablePath,
        batch: RecordBatch,
    ) -> Result<(), WriterError> {
        let writer = self.create_writer(table_path).await?;
        writer
            .append_arrow_batch(batch)
            .map_err(|source| WriterError::AppendArrowBatch {
                table_path: table_path.clone(),
                source: Box::new(source),
            })?;
        self.flush(&writer, table_path).await
    }

    fn get_connection(&self) -> Result<&FlussConnection, WriterError> {
        self.connection
            .as_ref()
            .ok_or(WriterError::ConnectionNotInitialized)
    }

    async fn get_table_by_path(
        &self,
        table_path: &TablePath,
    ) -> Result<FlussTable<'_>, WriterError> {
        let connection = self.get_connection()?;

        // The fluss get table API at the moment, doesn't return a specific error for table not
        // found, so we need to check if the table exists first.
        let table_id = connection
            .get_metadata()
            .fetch_table_id(table_path)
            .await
            .map_err(|source| WriterError::GetTableFailed {
                table_path: table_path.clone(),
                source: Box::new(source),
            })?;

        if table_id.is_none() {
            return Err(WriterError::TableNotFound {
                table_path: table_path.clone(),
            });
        }

        connection
            .get_table(table_path)
            .await
            .map_err(|source| match source {
                FlussError::FlussAPIError { api_error }
                    if api_error.code == fluss::rpc::FlussError::TableNotExist.code() =>
                {
                    WriterError::TableNotFound {
                        table_path: table_path.clone(),
                    }
                }
                _ => WriterError::GetTableFailed {
                    table_path: table_path.clone(),
                    source: Box::new(source),
                },
            })
    }

    async fn create_writer(&self, table_path: &TablePath) -> Result<AppendWriter, WriterError> {
        let table = self.get_table_by_path(table_path).await?;
        table
            .new_append()
            .map_err(|source| WriterError::CreateAppender {
                table_path: table_path.clone(),
                source: Box::new(source),
            })?
            .create_writer()
            .map_err(|source| WriterError::CreateWriter {
                table_path: table_path.clone(),
                source: Box::new(source),
            })
    }

    async fn flush(
        &self,
        writer: &AppendWriter,
        table_path: &TablePath,
    ) -> Result<(), WriterError> {
        writer
            .flush()
            .await
            .map_err(|source| WriterError::FlushRows {
                table_path: table_path.clone(),
                source: Box::new(source),
            })
    }
}

impl TableWriter for FlussWriter {
    async fn create_table_if_not_exists(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
    ) -> Result<(), WriterError> {
        self.get_connection()?
            .get_admin()
            .map_err(|source| WriterError::GetAdminClient {
                source: Box::new(source),
            })?
            .create_table(table_path, table_descriptor, true)
            .await
            .map_err(|source| WriterError::CreateTable {
                table_path: table_path.clone(),
                source: Box::new(source),
            })
    }

    async fn write_to_table(
        &self,
        table_path: &TablePath,
        op: Op,
        batch: RecordBatch,
    ) -> Result<(), WriterError> {
        match op {
            Op::Append => self.append_record_batch(table_path, batch).await,
        }
    }

    async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>, WriterError> {
        self.get_table_by_path(table_path).await
    }
}

#[cfg(test)]
mod tests {
    use fluss::{error::Error as FlussError, metadata::TablePath};
    use iggy_connector_sdk::Error as ConnectorError;

    use super::WriterError;

    #[test]
    fn given_connector_error_when_converting_should_preserve_original_variant() {
        let expected = ConnectorError::InvalidConfigValue("invalid value".to_string());

        let actual: ConnectorError = WriterError::Connector(expected.clone()).into();

        assert_eq!(actual, expected);
    }

    #[test]
    fn given_missing_connection_when_converting_should_return_init_error() {
        let error: ConnectorError = WriterError::ConnectionNotInitialized.into();

        assert_eq!(
            error,
            ConnectorError::InitError("Fluss connection is not initialized".to_string())
        );
    }

    #[test]
    fn given_arrow_append_failure_when_converting_should_return_cannot_store_data() {
        let error: ConnectorError = WriterError::AppendArrowBatch {
            table_path: TablePath::new("fluss", "iggy_messages"),
            source: Box::new(FlussError::WriterClosed {
                message: "writer closed".to_string(),
            }),
        }
        .into();

        assert!(matches!(
            error,
            ConnectorError::CannotStoreData(message)
                if message.contains("fluss.iggy_messages") && message.contains("writer closed")
        ));
    }
}
