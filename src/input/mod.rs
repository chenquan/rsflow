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

 
#[cfg(test)]
mod tests {
    use crate::input::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_noop_ack() {
        NoopAck::ack(&NoopAck).await;
    }

    #[tokio::test]
    async fn test_file_input_config() {
        let config = InputConfig::File(file::FileInputConfig {
            path: "test.txt".to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(false),
        });
        let input = InputConfig::build(&config).unwrap();
        // 验证构建的输入组件是否能够正确连接和关闭
        assert!(input.connect().await.is_err()); // 文件不存在，应该返回错误
    }

    #[tokio::test]
    async fn test_generate_input_config() {
        let config = InputConfig::Generate(serde_json::from_str::<generate::GenerateConfig>(r#"{
            "context": "test message",
            "interval": "10ms",
            "count": 5,
            "batch_size": 2
        }"#).unwrap());
        let input = InputConfig::build(&config).unwrap();
        // 验证生成输入组件能够正确连接
        assert!(input.connect().await.is_ok());
        // 读取消息并验证
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["test message", "test message"]);
        ack.ack().await;
        // 关闭连接
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_memory_input_config() {
        let messages = vec!["message1".to_string(), "message2".to_string()];
        let config = InputConfig::Memory(memory::MemoryInputConfig {
            messages: Some(messages),
        });
        let input = InputConfig::build(&config).unwrap();
        // 验证内存输入组件能够正确连接
        assert!(input.connect().await.is_ok());
        // 读取消息并验证
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message1"]);
        ack.ack().await;
        // 读取第二条消息
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message2"]);
        ack.ack().await;
        // 队列为空，应该返回Done错误
        assert!(matches!(input.read().await, Err(Error::Done)));
        // 关闭连接
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_sql_input_config() {
        let config = InputConfig::Sql(serde_json::from_str::<sql::SqlConfig>(r#"{
            "select_sql": "SELECT 1 as id, 'test' as name",
            "create_table_sql": "CREATE TABLE test (id INT, name VARCHAR)"
        }"#).unwrap());
        let input = InputConfig::build(&config).unwrap();
        // 验证SQL输入组件能够正确连接
        assert!(input.connect().await.is_ok());
        // 读取消息
        let (_, ack) = input.read().await.unwrap();
        ack.ack().await;
        // 再次读取应该返回Done错误，因为SQL输入只读取一次
        assert!(matches!(input.read().await, Err(Error::Done)));
        // 关闭连接
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_http_input_config() {
        let config = InputConfig::Http(http::HttpInputConfig {
            address: "127.0.0.1:0".to_string(), // 使用随机端口
            path: "/test".to_string(),
            cors_enabled: Some(false),
        });
        let input = InputConfig::build(&config).unwrap();
        // 验证HTTP输入组件能够正确连接
        assert!(input.connect().await.is_ok());
        // 关闭连接
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_input_config_invalid() {
        // 测试无效的文件路径
        let config = InputConfig::File(file::FileInputConfig {
            path: "".to_string(), // 空路径
            close_on_eof: Some(true),
            start_from_beginning: Some(false),
        });
        let input = InputConfig::build(&config);
        assert!(input.is_ok()); // 构建时不会检查路径有效性

        // 测试无效的HTTP地址
        let config = InputConfig::Http(http::HttpInputConfig {
            address: "invalid-address".to_string(), // 无效地址
            path: "/test".to_string(),
            cors_enabled: Some(false),
        });
        let input = InputConfig::build(&config);
        assert!(input.is_ok()); // 构建时不会检查地址有效性
    }
    
    #[tokio::test]
    async fn test_mqtt_input_config() {
        let config = InputConfig::Mqtt(mqtt::MqttInputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test-client".to_string(),
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            topics: vec!["test/topic".to_string()],
            qos: Some(1),
            clean_session: Some(true),
            keep_alive: Some(60),
        });
        let input = InputConfig::build(&config);
        assert!(input.is_ok()); // 验证MQTT配置能够正确构建输入组件
    }
    
    #[tokio::test]
    async fn test_kafka_input_config() {
        let config = InputConfig::Kafka(kafka::KafkaInputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: Some("test-client".to_string()),
            start_from_latest: false,
        });
        let input = InputConfig::build(&config);
        assert!(input.is_ok()); // 验证Kafka配置能够正确构建输入组件
    }
}