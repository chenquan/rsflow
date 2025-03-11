//! Output component module
//!
//! The output component is responsible for sending the processed data to the target system.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{Error, MessageBatch};

mod drop;
pub mod file;
pub mod http;
pub mod kafka;
pub mod mqtt;

pub mod stdout;

/// Feature interface of the output component
#[async_trait]
pub trait Output: Send + Sync {
    /// Connect to the output destination
    async fn connect(&self) -> Result<(), Error>;

    /// Write a message to the output destination
    async fn write(&self, msg: &MessageBatch) -> Result<(), Error>;

    /// Close the output destination connection
    async fn close(&self) -> Result<(), Error>;
}

/// Output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputConfig {
    File(file::FileOutputConfig),
    Http(http::HttpOutputConfig),
    Kafka(kafka::KafkaOutputConfig),
    Mqtt(mqtt::MqttOutputConfig),
    Stdout(stdout::StdoutOutputConfig),
    Drop,
}

impl OutputConfig {
    /// Build the output component according to the configuration
    pub fn build(&self) -> Result<Arc<dyn Output>, Error> {
        match self {
            OutputConfig::File(config) => Ok(Arc::new(file::FileOutput::new(config)?)),
            OutputConfig::Http(config) => Ok(Arc::new(http::HttpOutput::new(config)?)),
            OutputConfig::Kafka(config) => Ok(Arc::new(kafka::KafkaOutput::new(config)?)),
            OutputConfig::Mqtt(config) => Ok(Arc::new(mqtt::MqttOutput::new(config)?)),
            OutputConfig::Stdout(config) => Ok(Arc::new(stdout::StdoutOutput::new(config)?)),
            OutputConfig::Drop => Ok(Arc::new(drop::DropOutput)),
        }
    }
}
