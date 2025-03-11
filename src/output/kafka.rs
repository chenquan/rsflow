//! Kafka output component
//!
//! Send the processed data to the Kafka topic

use serde::{Deserialize, Serialize};

use crate::{Content, Error};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, "none"),
            CompressionType::Gzip => write!(f, "gzip"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Lz4 => write!(f, "lz4"),
        }
    }
}

/// Kafka output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOutputConfig {
    /// List of Kafka server addresses
    pub brokers: Vec<String>,
    /// Target topic
    pub topic: String,
    /// Partition key (optional)
    pub key: Option<String>,
    /// Client ID
    pub client_id: Option<String>,
    /// Compression type
    pub compression: Option<CompressionType>,
    /// Acknowledgment level (0=no acknowledgment, 1=leader acknowledgment, all=all replica acknowledgments)
    pub acks: Option<String>,
}

/// Kafka output component
pub struct KafkaOutput {
    config: KafkaOutputConfig,
    producer: Arc<RwLock<Option<FutureProducer>>>,
}

impl KafkaOutput {
    /// Create a new Kafka output component
    pub fn new(config: &KafkaOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            producer: Arc::new(RwLock::new(None)),
        })
    }
}
use crate::{output::Output, MessageBatch};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[async_trait]
impl Output for KafkaOutput {
    async fn connect(&self) -> Result<(), Error> {
        let mut client_config = ClientConfig::new();

        // Configure the Kafka server address
        client_config.set("bootstrap.servers", &self.config.brokers.join(","));

        // Set the client ID
        if let Some(client_id) = &self.config.client_id {
            client_config.set("client.id", client_id);
        }

        // Set the compression type
        if let Some(compression) = &self.config.compression {
            client_config.set("compression.type", compression.to_string().to_lowercase());
        }

        // Set the confirmation level
        if let Some(acks) = &self.config.acks {
            client_config.set("acks", acks);
        }

        // Create a producer
        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("A Kafka producer cannot be created: {}", e)))?;

        // Save the producer instance
        let producer_arc = self.producer.clone();
        let mut producer_guard = producer_arc.write().await;
        *producer_guard = Some(producer);

        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        let producer_arc = self.producer.clone();
        let producer_guard = producer_arc.read().await;
        let producer = producer_guard.as_ref().ok_or_else(|| {
            Error::Connection("The Kafka producer is not initialized".to_string())
        })?;

        let payloads = msg.as_string()?;
        if payloads.is_empty() {
            return Ok(());
        }

        match &msg.content {
            Content::Arrow(_) => {
                return Err(Error::Processing(
                    "The arrow format is not supported".to_string(),
                ))
            }
            Content::Binary(v) => {
                for x in v {
                    // 创建记录
                    let mut record = FutureRecord::to(&self.config.topic).payload(&x);

                    // 如果有分区键，则设置
                    if let Some(key) = &self.config.key {
                        record = record.key(key);
                    }

                    // Get the producer and send the message
                    producer
                        .send(record, Duration::from_secs(5))
                        .await
                        .map_err(|(e, _)| {
                            Error::Processing(format!("Failed to send a Kafka message: {}", e))
                        })?;
                }
            }
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // Get the producer and close
        let producer_arc = self.producer.clone();
        let mut producer_guard = producer_arc.write().await;

        if let Some(producer) = producer_guard.take() {
            // Wait for all messages to be sent
            producer.flush(Duration::from_secs(30)).map_err(|e| {
                Error::Connection(format!(
                    "Failed to refresh the message when the Kafka producer is disabled: {}",
                    e
                ))
            })?;
        }
        Ok(())
    }
}
