//! Input component module
//!
//! The input component is responsible for receiving data from various sources such as message queues, file systems, HTTP endpoints, and so on.

use crate::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock, RwLock};

pub mod file;
mod generate;
pub mod http;
pub mod kafka;
pub mod memory;
pub mod mqtt;
pub mod sql;

lazy_static::lazy_static! {
    static ref INPUT_BUILDERS: RwLock<HashMap<String, Arc<dyn InputBuilder>>> = RwLock::new(HashMap::new());
    static ref INITIALIZED: OnceLock<()> = OnceLock::new();
}

pub trait InputBuilder: Send + Sync {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Input>, Error>;
}

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

/// Input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    #[serde(rename = "type")]
    pub input_type: String,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl InputConfig {
    /// Building input components
    pub fn build(&self) -> Result<Arc<dyn Input>, Error> {
        let builders = INPUT_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.input_type) {
            builder.build(&self.config)
        } else {
            Err(Error::Config(format!(
                "Unknown input type: {}",
                self.input_type
            )))
        }
    }
}

pub fn register_input_builder(type_name: &str, builder: Arc<dyn InputBuilder>) {
    let mut builders = INPUT_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        panic!("Input type already registered: {}", type_name)
    }
    builders.insert(type_name.to_string(), builder);
}

pub fn get_registered_input_types() -> Vec<String> {
    let builders = INPUT_BUILDERS.read().unwrap();
    builders.keys().cloned().collect()
}

pub fn init() {
    INITIALIZED.get_or_init(|| {
        file::init();
        generate::init();
        http::init();
        kafka::init();
        memory::init();
        mqtt::init();
        sql::init();
    });
}

#[cfg(test)]
mod tests {
    use crate::input::*;

    #[tokio::test]
    async fn test_noop_ack() {
        NoopAck::ack(&NoopAck).await;
    }


    #[tokio::test]
    async fn test_file_input_config() {
        init();

        let config = file::FileInputConfig {
            path: "test.txt".to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(false),
        };
        let input_config = InputConfig {
            input_type: "file".to_string(),
            config: Some(serde_json::to_value(config).unwrap()),
        };
        let input = InputConfig::build(&input_config).unwrap();
        // Verify that the built input component can connect and close correctly
        assert!(input.connect().await.is_err()); // File does not exist, should return error
    }

    #[tokio::test]
    async fn test_generate_input_config() {
        init();

        let input_config = serde_yaml::from_str::<InputConfig>(
            r#"
"type": "generate"
"context": "test message"
"interval": "10ms"
"count": 5
"batch_size": 2
            "#,
        )
        .unwrap();

        let input = InputConfig::build(&input_config).unwrap();
        // Verify that the generate input component can connect correctly
        assert!(input.connect().await.is_ok());
        // Read message and verify
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(
            batch.as_string().unwrap(),
            vec!["test message", "test message"]
        );
        ack.ack().await;
        // Close connection
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_memory_input_config() {
        init();

        let messages = vec!["message1".to_string(), "message2".to_string()];
        let config = memory::MemoryInputConfig {
            messages: Some(messages),
        };

        let input_config = InputConfig {
            input_type: "memory".to_string(),
            config: Some(serde_json::to_value(config).unwrap()),
        };

        let input = InputConfig::build(&input_config).unwrap();
        // Verify that the memory input component can connect correctly
        assert!(input.connect().await.is_ok());
        // Read message and verify
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message1"]);
        ack.ack().await;
        // Read the second message
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message2"]);
        ack.ack().await;
        // Queue is empty, should return Done error
        assert!(matches!(input.read().await, Err(Error::Done)));
        // Close connection
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_sql_input_config() {
        init();

        let config = serde_json::from_str::<sql::SqlInputConfig>(
            r#"{
            "select_sql": "SELECT 1 as id, 'test' as name",
            "create_table_sql": "CREATE TABLE test (id INT, name VARCHAR)"
        }"#,
        )
        .unwrap();

        let input_config = InputConfig {
            input_type: "sql".to_string(),
            config: Some(serde_json::to_value(config).unwrap()),
        };
        let input = InputConfig::build(&input_config).unwrap();
        // Verify that the SQL input component can connect correctly
        assert!(input.connect().await.is_ok());
        // Read message
        let (_, ack) = input.read().await.unwrap();
        ack.ack().await;
        // Reading again should return Done error, because SQL input only reads once
        assert!(matches!(input.read().await, Err(Error::Done)));
        // Close connection
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_http_input_config() {
        init();

        let config = http::HttpInputConfig {
            address: "127.0.0.1:0".to_string(), // 使用随机端口
            path: "/test".to_string(),
            cors_enabled: Some(false),
        };

        let input_config = InputConfig {
            input_type: "http".to_string(),
            config: Some(serde_json::to_value(config).unwrap()),
        };
        let input = InputConfig::build(&input_config).unwrap();
        // Verify that the HTTP input component can connect correctly
        assert!(input.connect().await.is_ok());
        // Close connection
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_input_config_invalid() {
        init();

        // 测试无效的文件路径
        let config = file::FileInputConfig {
            path: "".to_string(), // 空路径
            close_on_eof: Some(true),
            start_from_beginning: Some(false),
        };

        let input_config = InputConfig {
            input_type: "file".to_string(),
            config: Some(serde_json::to_value(config).unwrap()),
        };
        let input = InputConfig::build(&input_config);
        assert!(input.is_ok()); // Path validity is not checked during construction

        // 测试无效的HTTP地址
        let config = http::HttpInputConfig {
            address: "invalid-address".to_string(), // 无效地址
            path: "/test".to_string(),
            cors_enabled: Some(false),
        };
        let input_config = InputConfig {
            input_type: "http".to_string(),
            config: Some(serde_json::to_value(config).unwrap()),
        };
        let input = InputConfig::build(&input_config);
        assert!(input.is_ok()); // Address validity is not checked during construction
    }

    #[tokio::test]
    async fn test_mqtt_input_config() {
        init();

        let config = mqtt::MqttInputConfig {
            host: "localhost".to_string(),
            port: 1883,
            client_id: "test-client".to_string(),
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            topics: vec!["test/topic".to_string()],
            qos: Some(1),
            clean_session: Some(true),
            keep_alive: Some(60),
        };

        let input_config = InputConfig {
            input_type: "mqtt".to_string(),
            config: Some(serde_json::to_value(config).unwrap()),
        };
        let input = InputConfig::build(&input_config);
        assert!(input.is_ok()); // Verify that MQTT configuration can correctly build input component
    }

    #[tokio::test]
    async fn test_kafka_input_config() {
        init();

        let config = kafka::KafkaInputConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["test-topic".to_string()],
            consumer_group: "test-group".to_string(),
            client_id: Some("test-client".to_string()),
            start_from_latest: false,
        };
        let input_config = InputConfig {
            input_type: "kafka".to_string(),
            config: Some(serde_json::to_value(config).unwrap()),
        };
        let input = InputConfig::build(&input_config);
        assert!(input.is_ok()); // Verify that Kafka configuration can correctly build input component
    }
}
