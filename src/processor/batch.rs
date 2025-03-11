//! Batch Processor Components
//!
//! Batch multiple messages into one or more messages

use crate::{processor::Processor, Content, Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Batch processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchProcessorConfig {
    /// Batch size
    pub count: usize,
    /// Batch timeout (ms)
    pub timeout_ms: u64,
    /// Batch data type
    pub data_type: String,
}

/// Batch Processor Components
pub struct BatchProcessor {
    config: BatchProcessorConfig,
    batch: Arc<RwLock<Vec<MessageBatch>>>,
    last_batch_time: Arc<Mutex<std::time::Instant>>,
}

impl BatchProcessor {
    /// Create a new batch processor component
    pub fn new(config: &BatchProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            batch: Arc::new(RwLock::new(Vec::with_capacity(config.count))),
            last_batch_time: Arc::new(Mutex::new(std::time::Instant::now())),
        })
    }

    /// Check if the batch should be refreshed
    async fn should_flush(&self) -> bool {
        // 如果批处理已满，则刷新
        let batch = self.batch.read().await;
        if batch.len() >= self.config.count {
            return true;
        }
        let last_batch_time = self.last_batch_time.lock().await;
        // 如果超过超时时间且批处理不为空，则刷新
        if !batch.is_empty()
            && last_batch_time.elapsed().as_millis() >= self.config.timeout_ms as u128
        {
            return true;
        }

        false
    }

    /// Refresh the batch
    async fn flush(&self) -> Result<Vec<MessageBatch>, Error> {
        let mut batch = self.batch.write().await;

        if batch.is_empty() {
            return Ok(vec![]);
        }

        // Create a new batch message
        let new_batch = match self.config.data_type.as_str() {
            "arrow" => {
                let mut combined_content = Vec::new();

                for msg in batch.iter() {
                    if let Content::Arrow(v) = &msg.content {
                        combined_content.push(v.clone());
                    }
                }
                let schema = combined_content[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &combined_content)
                    .map_err(|e| Error::Processing(format!("Merge batches failed: {}", e)))?;
                Ok(vec![MessageBatch::new_arrow(batch)])
            }
            "binary" => {
                let mut combined_content = Vec::new();

                for msg in batch.iter() {
                    if let Content::Binary(v) = &msg.content {
                        combined_content.extend(v.clone());
                    }
                }
                Ok(vec![MessageBatch::new_binary(combined_content)])
            }
            _ => Err(Error::Processing("Invalid data type".to_string())),
        };

        batch.clear();
        let mut last_batch_time = self.last_batch_time.lock().await;

        *last_batch_time = std::time::Instant::now();

        new_batch
    }
}

#[async_trait]
impl Processor for BatchProcessor {
    async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        match &msg.content {
            Content::Arrow(_) => {
                if self.config.data_type != "arrow" {
                    return Err(Error::Processing("Invalid data type".to_string()));
                }
            }
            Content::Binary(_) => {
                if self.config.data_type != "binary" {
                    return Err(Error::Processing("Invalid data type".to_string()));
                }
            }
        }

        {
            let mut batch = self.batch.write().await;

            // Add messages to a batch
            batch.push(msg);
        }

        // Check if the batch should be refreshed
        if self.should_flush().await {
            self.flush().await
        } else {
            // If it is not refreshed, an empty result is returned
            Ok(vec![])
        }
    }

    async fn close(&self) -> Result<(), Error> {
        let mut batch = self.batch.write().await;

        batch.clear();
        Ok(())
    }
}
