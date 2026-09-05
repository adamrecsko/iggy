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

use std::{collections::HashMap, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema, json::ReaderBuilder};
use fluss::metadata::TablePath;
use iggy_connector_sdk::{ConsumedMessage, Payload};
use simd_json::prelude::*;
use tracing::warn;

use super::Error;
use crate::{
    FlussSinkConfig,
    schema_catalog::SchemaCatalog,
    writer::{Op, Stat, TableWriter},
};

#[derive(Debug)]
pub(crate) struct MultiTableRouter {
    schema_catalog: SchemaCatalog,
    auto_create_tables: bool,
    route_key: String,
}

impl MultiTableRouter {
    pub(crate) fn new(config: &FlussSinkConfig) -> Self {
        Self {
            schema_catalog: SchemaCatalog::default(),
            auto_create_tables: config.auto_create_table,
            route_key: config.route_key.clone(),
        }
    }

    pub(crate) async fn route(
        &self,
        writer: &impl TableWriter,
        messages: Vec<ConsumedMessage>,
    ) -> Result<Stat, Error> {
        let mut result = Stat::default();
        let (partitioned_messages, stat) =
            partition_messages_by_route_key(messages, &self.route_key);
        result = result.add(stat);
        for (table, messages) in partitioned_messages {
            let entry = match self.schema_catalog.get_schema_entry(&table) {
                Some(schema) => schema,
                None => match self
                    .schema_catalog
                    .create_and_store_entry_from_table(&table, writer)
                    .await?
                {
                    Some(schema) => schema,
                    None if self.auto_create_tables => {
                        let entry = self
                            .schema_catalog
                            .create_and_store_schema_from_infer(&table, &messages)?;
                        writer
                            .create_table_if_not_exists(&table, &entry.table_descriptor)
                            .await?;
                        entry
                    }
                    None => {
                        warn!(
                            "FlussSink: Skipping messages for table [{}] because schema could not found in the fluss cluster and auto_create_tables is disabled.",
                            table
                        );
                        result.inc_err_by(messages.len() as u64);
                        continue;
                    }
                },
            };

            if let Some(batch) = to_record_batch(messages, Arc::clone(&entry.schema))? {
                result.appended += batch.num_rows() as u64;
                writer.write_to_table(&table, Op::Append, batch).await?;
            }
        }

        Ok(result)
    }
}

fn to_record_batch(
    messages: Vec<ConsumedMessage>,
    schema: Arc<Schema>,
) -> Result<Option<RecordBatch>, Error> {
    let mut decoder = ReaderBuilder::new(schema)
        .with_strict_mode(false)
        .with_ignore_type_conflicts(false)
        .build_decoder()
        .map_err(|err| Error::FailedToCreateRecordBatch {
            reason: err.to_string(),
        })?;

    let payloads: Vec<&simd_json::OwnedValue> = messages
        .iter()
        .filter_map(|message| match &message.payload {
            Payload::Json(payload) => Some(payload),
            _ => None,
        })
        .collect();

    decoder
        .serialize(payloads.as_slice())
        .map_err(|err| Error::FailedToCreateRecordBatch {
            reason: err.to_string(),
        })?;

    decoder
        .flush()
        .map_err(|err| Error::FailedToCreateRecordBatch {
            reason: err.to_string(),
        })
}

fn to_table_path(table: &str) -> Result<TablePath, Error> {
    let parts = table.split('.').collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid table format, expected 'database.table', got {table}"),
        });
    }
    let database = parts[0];
    let table_name = parts[1];

    if let Some(reason) = TablePath::detect_invalid_name(database) {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid name detected, for {database} : {reason}"),
        });
    }

    if let Some(reason) = TablePath::detect_invalid_name(table_name) {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid name detected, for {table_name} : {reason}"),
        });
    }

    if let Some(reason) = TablePath::validate_prefix(database) {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid name detected, for {database} : {reason}"),
        });
    }

    if let Some(reason) = TablePath::validate_prefix(table_name) {
        return Err(Error::FailedToExtractTablePath {
            reason: format!("Invalid name detected, for {table_name} : {reason}"),
        });
    }

    Ok(TablePath::new(database, table_name))
}

fn extract_string_field(message: &ConsumedMessage, route_key: &str) -> Result<String, Error> {
    let id = message.id;
    match &message.payload {
        Payload::Json(payload) => payload
            .as_object()
            .and_then(|obj| obj.get(route_key))
            .and_then(|value| value.as_str())
            .map(str::to_owned)
            .ok_or(Error::ExtractStringField {
                id,
                key: route_key.to_owned(),
                reason: "Value not found".to_owned(),
            }),
        _ => Err(Error::ExtractStringField {
            id,
            key: route_key.to_owned(),
            reason: "Payload type is not supported, only Json type is supported.".to_owned(),
        }),
    }
}

fn partition_messages_by_route_key<I>(
    messages: I,
    route_key: &str,
) -> (HashMap<TablePath, Vec<ConsumedMessage>>, Stat)
where
    I: IntoIterator<Item = ConsumedMessage>,
{
    let mut table_to_message = HashMap::<TablePath, Vec<ConsumedMessage>>::new();
    let mut extract = Stat::default();
    let mut path_convert = Stat::default();

    messages
        .into_iter()
        .filter_map(|message| match extract_string_field(&message, route_key) {
            Ok(route_target) => Some((route_target, message)),
            Err(error) => {
                extract.inc_err();
                warn!(
                    "FlussSink: Skipping message [{}] because route key extraction failed: {}",
                    message.id, error
                );
                None
            }
        })
        .filter_map(|(table, message)| match to_table_path(&table) {
            Ok(table_path) => Some((table_path, message)),
            Err(error) => {
                path_convert.inc_err();
                warn!(
                    "FlussSink: Skipping message [{}] because table path extraction failed: {}",
                    message.id, error
                );
                None
            }
        })
        .for_each(|(table_path, message)| {
            table_to_message
                .entry(table_path)
                .or_default()
                .push(message)
        });

    (table_to_message, extract.add(path_convert))
}
