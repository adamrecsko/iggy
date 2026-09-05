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
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arrow::{datatypes::Schema, error::ArrowError, json::reader::infer_json_schema_from_iterator};
use fluss::{
    metadata::{DataField, RowType, SchemaBuilder, TableDescriptor, TablePath},
    record::from_arrow_field,
};
use iggy_connector_sdk::{ConsumedMessage, Payload};
use thiserror::Error;

use crate::{
    static_schema::{SingleTableConfig, SingleTableLayout},
    writer::{self, TableWriter},
};

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Writer(#[from] writer::WriterError),
    #[error("Sampling has failed for messages. Can not create arrow schema because of [{reason}]")]
    SampleFailed { reason: String },
    #[error("Failed to build Fluss table descriptor for table {table_path}: {reason}")]
    BuildTableDescriptor {
        table_path: TablePath,
        reason: String,
    },
    #[error(
        "Failed to convert arrow Schema to fluss RowType for table {table_path} because of: {reason}"
    )]
    ArrowSchemaToFlussRowType {
        table_path: TablePath,
        reason: String,
    },
    #[error("Failed to convert Fluss schema to Arrow schema because: {reason}")]
    FlussToArrowSchemaFailed { reason: String },
}

#[derive(Debug)]
pub(crate) struct SchemaEntry {
    pub(crate) schema: Arc<Schema>,
    pub(crate) table_descriptor: Arc<TableDescriptor>,
}
#[derive(Debug, Default)]
pub(crate) struct SchemaCatalog {
    table_to_schema: Mutex<HashMap<TablePath, Arc<SchemaEntry>>>,
}

impl SchemaCatalog {
    pub(crate) fn get_schema_entry(&self, table: &TablePath) -> Option<Arc<SchemaEntry>> {
        self.table_to_schema
            .lock()
            .expect("table_schema mutex poisoned")
            .get(table)
            .cloned()
    }

    pub(crate) fn create_and_store_static_schema_entry(
        &self,
        table: &TablePath,
        config: &SingleTableConfig,
    ) -> Result<Arc<SchemaEntry>, Error> {
        let table_layout = SingleTableLayout::from_single_table_config(config);
        let table_descriptor =
            table_layout
                .build_table_descriptor()
                .map_err(|err| Error::BuildTableDescriptor {
                    table_path: table.clone(),
                    reason: err.to_string(),
                })?;

        let row_type = table_descriptor.schema().row_type();

        let schema = fluss::record::to_arrow_schema(row_type).map_err(|error| {
            Error::FlussToArrowSchemaFailed {
                reason: error.to_string(),
            }
        })?;

        Ok(self.create_and_store(table, schema, Arc::new(table_descriptor)))
    }

    pub(crate) async fn create_and_store_entry_from_table(
        &self,
        table: &TablePath,
        writer: &impl TableWriter,
    ) -> Result<Option<Arc<SchemaEntry>>, Error> {
        let fluss_table = writer.get_table(table).await;
        let fluss_table = match fluss_table {
            Ok(table) => table,
            Err(writer::WriterError::TableNotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        let descriptor = fluss_table
            .get_table_info()
            .to_table_descriptor()
            .map_err(|error| Error::BuildTableDescriptor {
                table_path: table.clone(),
                reason: error.to_string(),
            })?;

        let schema = fluss::record::to_arrow_schema(&fluss_table.get_table_info().row_type)
            .map_err(|error| Error::FlussToArrowSchemaFailed {
                reason: error.to_string(),
            })?;

        Ok(Some(self.create_and_store(
            table,
            schema,
            Arc::new(descriptor),
        )))
    }

    pub(crate) fn create_and_store_schema_from_infer(
        &self,
        table: &TablePath,
        messages: &[ConsumedMessage],
    ) -> Result<Arc<SchemaEntry>, Error> {
        let schema = sample_and_infer_schema(messages)?;
        let table_descriptor = table_descriptor_from_schema(&schema, table)?;
        Ok(self.create_and_store(table, Arc::new(schema), Arc::new(table_descriptor)))
    }

    fn create_and_store(
        &self,
        table: &TablePath,
        schema: Arc<Schema>,
        table_descriptor: Arc<TableDescriptor>,
    ) -> Arc<SchemaEntry> {
        let entry = Arc::new(SchemaEntry {
            schema,
            table_descriptor,
        });
        self.table_to_schema
            .lock()
            .expect("table_schema mutex poisoned")
            .insert(table.to_owned(), Arc::clone(&entry));
        entry
    }
}

fn table_descriptor_from_schema(
    schema: &Schema,
    table: &TablePath,
) -> Result<TableDescriptor, Error> {
    let row_type = schema_to_row_type(schema, table)?;
    let fluss_schema = SchemaBuilder::new()
        .with_row_type(&fluss::metadata::DataType::Row(row_type))
        .build()
        .map_err(|error| Error::BuildTableDescriptor {
            table_path: table.clone(),
            reason: error.to_string(),
        })?;

    TableDescriptor::builder()
        .comment("Automatically created table")
        .schema(fluss_schema)
        .build()
        .map_err(|error| Error::BuildTableDescriptor {
            table_path: table.clone(),
            reason: error.to_string(),
        })
}

fn schema_to_row_type(schema: &Schema, table: &TablePath) -> Result<RowType, Error> {
    schema
        .fields()
        .iter()
        .map(|field| {
            from_arrow_field(field)
                .map(|data_type| DataField::new(field.name().clone(), data_type, None))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(RowType::new)
        .map_err(|error| Error::ArrowSchemaToFlussRowType {
            table_path: table.clone(),
            reason: error.to_string(),
        })
}

fn sample_and_infer_schema(messages: &[ConsumedMessage]) -> Result<Schema, Error> {
    let sampler = messages.iter().map(|message| {
        let id = message.id;
        match &message.payload {
            Payload::Json(value) => {
                simd_json::serde::from_refowned_value::<serde_json::Value>(value)
                    .map_err(|error| ArrowError::ExternalError(Box::new(error)))
            }
            _ => Err(ArrowError::ExternalError(Box::new(Error::SampleFailed {
                reason: format!("Only JSON Payload is supported for sampling and infer [{id}]"),
            }))),
        }
    });

    infer_json_schema_from_iterator(sampler).map_err(|error| Error::SampleFailed {
        reason: error.to_string(),
    })
}
