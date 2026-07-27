use std::fmt::{self, Display, Formatter};

use fluss::{
    client::FlussConnection,
    metadata::{TableDescriptor, TablePath},
};
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, TopicMetadata};
use tracing::info;

use crate::schema::IggyDefaultTable;

pub struct FlussWriter {
    connection: Option<FlussConnection>,
    bootstrap_servers: String,
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
    fn get_connection(&self) -> Result<&FlussConnection, iggy_connector_sdk::Error> {
        self.connection.as_ref().ok_or_else(|| {
            iggy_connector_sdk::Error::InitError("Database not connected".to_string())
        })
    }

    pub async fn connect(&mut self) -> Result<(), iggy_connector_sdk::Error> {
        let config = fluss::config::Config {
            bootstrap_servers: self.bootstrap_servers.clone(),
            ..fluss::config::Config::default()
        };
        let connection = FlussConnection::new(config).await.map_err(|e| {
            iggy_connector_sdk::Error::Connection(
                format!("Can not connect to fluss server {e}").to_string(),
            )
        })?;
        self.connection = Some(connection);
        Ok(())
    }

    pub fn new(bootstrap_servers: String) -> Self {
        Self {
            bootstrap_servers,
            connection: None,
        }
    }
    async fn create_table_if_not_exists(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
    ) -> Result<(), iggy_connector_sdk::Error> {
        self.get_connection()?
            .get_admin()
            .map_err(|_| iggy_connector_sdk::Error::InitError("Can't get admin".to_string()))?
            .create_table(table_path, table_descriptor, true)
            .await
            .map_err(|_| iggy_connector_sdk::Error::InitError("Can not create table".to_string()))
    }

    pub async fn write_to_table(
        &self,
        table_path: TablePath,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
        topic_metadata: &TopicMetadata,
    ) -> Result<(), iggy_connector_sdk::Error> {
        let table_converter = IggyDefaultTable::default();
        let schema = table_converter.create_schema().map_err(|_| {
            iggy_connector_sdk::Error::InitError("Can not not create schema".to_string())
        })?;
        let table_descriptor = table_converter.create_table_descriptor().map_err(|_| {
            iggy_connector_sdk::Error::InitError("Can not not create table descriptor".to_string())
        })?;

        self.create_table_if_not_exists(&table_path, &table_descriptor)
            .await
            .map_err(|_| {
                iggy_connector_sdk::Error::InitError("Can not create table".to_string())
            })?;

        let table = self
            .get_connection()?
            .get_table(&table_path)
            .await
            .map_err(|_| iggy_connector_sdk::Error::InitError("Can not get table".to_string()))?;

        let writer = table
            .new_append()
            .map_err(|e| {
                iggy_connector_sdk::Error::InitError(
                    format!("Can not create appender {e}").to_string(),
                )
            })?
            .create_writer()
            .map_err(|_| iggy_connector_sdk::Error::InitError("Can't create writer".to_string()))?;

        for message in messages {
            let row = table_converter
                .create_generic_row(&schema, &message, &messages_metadata, topic_metadata)
                .map_err(|_| {
                    iggy_connector_sdk::Error::InitError("Can create generic row".to_string())
                })?;

            writer
                .append(&row)
                .map_err(|error| {
                    iggy_connector_sdk::Error::CannotStoreData(format!(
                        "Failed to append Fluss row: {error}"
                    ))
                })?
                .await
                .map_err(|error| {
                    iggy_connector_sdk::Error::CannotStoreData(format!(
                        "Failed to write Fluss row: {error}"
                    ))
                })?;
        }

        writer
            .flush()
            .await
            .map_err(|_| iggy_connector_sdk::Error::InitError("Can't flush rows".to_string()))?;

        Ok(())
    }
}
