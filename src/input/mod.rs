//! Input the component module
//!
//! The input component is responsible for receiving data from various sources such as message queues, file systems, HTTP endpoints, and so on.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{Error, MessageBatch};

pub mod file;
mod generate;
pub mod http;
pub mod kafka;
pub mod memory;
pub mod mqtt;
mod sql;

#[async_trait]
pub trait Ack: Send + Sync {
    async fn ack(&self);
}

#[async_trait]
pub trait Input: Send + Sync {
    /// Connect to the input source
    async fn connect(&self) -> Result<(), Error>;

    /// Read the message from the input source
    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error>;

    /// Close the input source connection
    async fn close(&self) -> Result<(), Error>;
}

pub struct NoopAck;

#[async_trait]
impl Ack for NoopAck {
    async fn ack(&self) {}
}

/// Input the configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputConfig {
    File(file::FileInputConfig),
    Http(http::HttpInputConfig),
    Kafka(kafka::KafkaInputConfig),
    Generate(generate::GenerateConfig),
    Memory(memory::MemoryInputConfig),
    Mqtt(mqtt::MqttInputConfig),
    Sql(sql::SqlConfig),
}

impl InputConfig {
    /// Build the input components according to the configuration
    pub fn build(&self) -> Result<Arc<dyn Input>, Error> {
        match self {
            InputConfig::File(config) => Ok(Arc::new(file::FileInput::new(config)?)),
            InputConfig::Http(config) => Ok(Arc::new(http::HttpInput::new(config)?)),
            InputConfig::Kafka(config) => Ok(Arc::new(kafka::KafkaInput::new(config)?)),
            InputConfig::Memory(config) => Ok(Arc::new(memory::MemoryInput::new(config)?)),
            InputConfig::Mqtt(config) => Ok(Arc::new(mqtt::MqttInput::new(config)?)),
            InputConfig::Generate(config) => {
                Ok(Arc::new(generate::GenerateInput::new(config.clone())?))
            }
            InputConfig::Sql(config) => Ok(Arc::new(sql::SqlInput::new(config)?)),
        }
    }
}
