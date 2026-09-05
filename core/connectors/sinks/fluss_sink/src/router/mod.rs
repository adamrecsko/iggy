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

mod multi_table;
mod single_table;

use iggy_connector_sdk::Error as ConnectorError;
use thiserror::Error;

use crate::{
    FlussSinkConfig,
    config::RouterType,
    schema_catalog::Error as SchemaCatalogError,
    writer::{self},
};

pub(crate) use multi_table::MultiTableRouter;
pub(crate) use single_table::SingleTableRouter;

#[derive(Debug)]
pub(crate) enum Router {
    SingleTable(SingleTableRouter),
    MultiTable(MultiTableRouter),
}

impl Router {
    pub(crate) fn from_config(config: &FlussSinkConfig) -> Router {
        match config.router_type {
            RouterType::Single => Router::SingleTable(SingleTableRouter::new(config)),
            RouterType::Multi => Router::MultiTable(MultiTableRouter::new(config)),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    WriterError(writer::WriterError),

    #[error("Failed to extract string value for [{key}] for message: [{id}] because: [{reason}]")]
    ExtractStringField {
        id: u128,
        key: String,
        reason: String,
    },

    #[error(transparent)]
    SchemaCatalog(#[from] SchemaCatalogError),

    #[error("Failed to create table path: [{reason}]")]
    FailedToExtractTablePath { reason: String },

    #[error("Failed to create table path: [{reason}]")]
    FailedToCreateRecordBatch { reason: String },
}

impl From<writer::WriterError> for Error {
    fn from(error: writer::WriterError) -> Self {
        Self::WriterError(error)
    }
}

impl From<Error> for ConnectorError {
    fn from(error: Error) -> Self {
        let message = error.to_string();
        match error {
            Error::WriterError(_) | Error::SchemaCatalog(SchemaCatalogError::Writer(_)) => {
                Self::InitError(message)
            }

            Error::ExtractStringField { .. }
            | Error::SchemaCatalog(_)
            | Error::FailedToExtractTablePath { .. }
            | Error::FailedToCreateRecordBatch { .. } => Self::InvalidRecordValue(message),
        }
    }
}
