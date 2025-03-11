//! Rust流处理引擎

use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod buffer;
pub mod config;
pub mod input;
pub mod metrics;
pub mod output;
pub mod pipeline;
pub mod processor;
pub mod stream;

/// 表示流处理引擎中的错误
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("序列化错误: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("配置错误: {0}")]
    Config(String),

    #[error("读取错误: {0}")]
    Reading(String),

    #[error("处理错误: {0}")]
    Processing(String),

    #[error("连接错误: {0}")]
    Connection(String),

    #[error("连接断开")]
    Disconnection,

    #[error("超时错误")]
    Timeout,

    #[error("未知错误: {0}")]
    Unknown(String),
    #[error("完成")]
    Done,
}

/// 消息元数据，存储消息的附加信息
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    fields: std::collections::HashMap<String, String>,
}

impl Metadata {
    /// 创建一个新的空元数据
    pub fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
        }
    }

    /// 设置元数据字段
    pub fn set(&mut self, key: &str, value: &str) {
        self.fields.insert(key.to_string(), value.to_string());
    }

    /// 获取元数据字段
    pub fn get(&self, key: &str) -> Option<&String> {
        self.fields.get(key)
    }

    /// 删除元数据字段
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.fields.remove(key)
    }
}

type Bytes = Vec<u8>;

/// 表示流处理引擎中的消息
#[derive(Clone, Debug)]
pub struct MessageBatch {
    /// 消息内容
    content: Content,
}

#[derive(Clone, Debug)]
pub enum Content {
    Arrow(RecordBatch),
    Binary(Vec<Bytes>),
}

impl MessageBatch {
    pub fn new_binary(content: Vec<Bytes>) -> Self {
        Self {
            content: Content::Binary(content),
        }
    }
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Ok(Self::new_binary(vec![content]))
    }
    pub fn new_arrow(content: RecordBatch) -> Self {
        Self {
            content: Content::Arrow(content),
        }
    }

    /// 从字符串创建消息
    pub fn from_string(content: &str) -> Self {
        Self::new_binary(vec![content.as_bytes().to_vec()])
    }

    /// 将消息内容解析为字符串
    pub fn as_string(&self) -> Result<Vec<String>, Error> {
        match &self.content {
            Content::Arrow(_) => Err(Error::Processing("无法解析为JSON".to_string())),
            Content::Binary(v) => {
                let x: Result<Vec<String>, Error> = v
                    .iter()
                    .map(|v| {
                        String::from_utf8(v.clone())
                            .map_err(|_| Error::Processing("无法解析为字符串".to_string()))
                    })
                    .collect();
                Ok(x?)
            }
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn len(&self) -> usize {
        match &self.content {
            Content::Arrow(v) => v.num_rows(),
            Content::Binary(v) => v.len(),
        }
    }
}
