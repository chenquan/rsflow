//! Processor component module
//!
//! The processor component is responsible for transforming, filtering, enriching, and so on.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{Error, MessageBatch};

pub mod batch;
pub mod json;
pub mod protobuf;
pub mod sql;
mod udf;

/// Characteristic interface of the processor component
#[async_trait]
pub trait Processor: Send + Sync {
    /// Process messages
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error>;

    /// Turn off the processor
    async fn close(&self) -> Result<(), Error>;
}

/// Processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProcessorConfig {
    Batch(batch::BatchProcessorConfig),
    Sql(sql::SqlProcessorConfig),
    JsonToArrow,
    ArrowToJson,
    Protobuf(protobuf::ProtobufProcessorConfig),
}

impl ProcessorConfig {
    /// Build the processor components according to the configuration
    pub fn build(&self) -> Result<Arc<dyn Processor>, Error> {
        match self {
            ProcessorConfig::Batch(config) => Ok(Arc::new(batch::BatchProcessor::new(config)?)),
            ProcessorConfig::Sql(config) => Ok(Arc::new(sql::SqlProcessor::new(config)?)),
            ProcessorConfig::JsonToArrow => Ok(Arc::new(json::JsonToArrowProcessor)),
            ProcessorConfig::ArrowToJson => Ok(Arc::new(json::ArrowToJsonProcessor)),
            ProcessorConfig::Protobuf(config) => {
                Ok(Arc::new(protobuf::ProtobufProcessor::new(config)?))
            }
        }
    }
}
