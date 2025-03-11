mod memory;

use crate::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// 缓冲区组件的特征接口
#[async_trait]
pub trait Buffer: Send + Sync {
    /// 将消息推入缓冲区
    async fn push(&self, msg: &MessageBatch) -> Result<(), Error>;

    /// 从缓冲区弹出消息
    async fn pop(&self) -> Result<Option<MessageBatch>, Error>;

    /// 关闭缓冲区
    async fn close(&self) -> Result<(), Error>;
}

/// 缓冲区配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BufferConfig {
    Memory(memory::MemoryBufferConfig),
    // Disk(disk::DiskBufferConfig),
    // Redis(redis::RedisBufferConfig),
}

impl BufferConfig {
    /// 根据配置构建缓冲区组件
    pub fn build(&self) -> Result<Arc<dyn Buffer>, Error> {
        match self {
            BufferConfig::Memory(config) => Ok(Arc::new(memory::MemoryBuffer::new(config)?)),
            // BufferConfig::Disk(config) => Ok(Arc::new(disk::DiskBuffer::new(config)?)),
            // BufferConfig::Redis(config) => Ok(Arc::new(redis::RedisBuffer::new(config)?)),
        }
    }
}
