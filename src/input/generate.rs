use crate::input::{Ack, Input, NoopAck};
use crate::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateConfig {
    context: String,
    #[serde(deserialize_with = "deserialize_duration")]
    interval: Duration,
    count: Option<usize>,
    batch_size: Option<usize>,
}

pub struct GenerateInput {
    config: GenerateConfig,
    count: AtomicI64,
    batch_size: usize,
}
impl GenerateInput {
    pub fn new(config: GenerateConfig) -> Result<Self, Error> {
        let batch_size = config.batch_size.unwrap_or(1);

        Ok(Self {
            config,
            count: AtomicI64::new(0),
            batch_size,
        })
    }
}

#[async_trait]
impl Input for GenerateInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        tokio::time::sleep(self.config.interval).await;

        if let Some(count) = self.config.count {
            let current_count = self.count.load(Ordering::SeqCst);
            if current_count >= count as i64 {
                return Err(Error::Done);
            }
            // 检查添加当前批次后是否会超过总数限制
            if current_count + self.batch_size as i64 > count as i64 {
                return Err(Error::Done);
            }
        }
        let mut msgs = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            let s = self.config.context.clone();
            msgs.push(s.into_bytes())
        }

        self.count
            .fetch_add(self.batch_size as i64, Ordering::SeqCst);

        Ok((MessageBatch::new_binary(msgs), Arc::new(NoopAck)))
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{generate::GenerateConfig, generate::GenerateInput, Input};
    use crate::Error;
    use std::time::Duration;

    #[tokio::test]
    async fn test_generate_input_new() {
        // 测试创建GenerateInput实例
        let config = GenerateConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();
        assert_eq!(input.batch_size, 2);
    }

    #[tokio::test]
    async fn test_generate_input_default_batch_size() {
        // 测试默认批处理大小
        let config = GenerateConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: None,
        };
        let input = GenerateInput::new(config).unwrap();
        assert_eq!(input.batch_size, 1); // 默认批处理大小应为1
    }

    #[tokio::test]
    async fn test_generate_input_connect() {
        // 测试连接方法
        let config = GenerateConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();
        assert!(input.connect().await.is_ok()); // 连接应该成功
    }

    #[tokio::test]
    async fn test_generate_input_read() {
        // 测试读取消息
        let config = GenerateConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();

        // 读取第一批消息
        let (batch, ack) = input.read().await.unwrap();
        let messages = batch.as_binary();
        assert_eq!(messages.len(), 2); // 批处理大小为2
        for msg in messages {
            assert_eq!(String::from_utf8(msg.to_vec()).unwrap(), "test message");
        }
        ack.ack().await;

        // 读取第二批消息
        let (batch, ack) = input.read().await.unwrap();
        let messages = batch.as_binary();
        assert_eq!(messages.len(), 2);
        ack.ack().await;

        // 读取第三批消息 (已达到count=5的上限，因为2+2+2>5)
        let result = input.read().await;
        assert!(matches!(result, Err(Error::Done)));
    }

    #[tokio::test]
    async fn test_generate_input_without_count_limit() {
        // 测试没有消息数量限制的情况
        let config = GenerateConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: None, // 无限制
            batch_size: Some(1),
        };
        let input = GenerateInput::new(config).unwrap();

        // 可以连续读取多次
        for _ in 0..10 {
            let result = input.read().await;
            assert!(result.is_ok());
            let (batch, ack) = result.unwrap();
            let messages = batch.as_binary();
            assert_eq!(messages.len(), 1);
            ack.ack().await;
        }
    }

    #[tokio::test]
    async fn test_generate_input_close() {
        // 测试关闭连接
        let config = GenerateConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(5),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();
        assert!(input.close().await.is_ok()); // 关闭应该成功
    }

    #[tokio::test]
    async fn test_generate_input_exact_count() {
        // 测试精确的计数限制
        let config = GenerateConfig {
            context: "test message".to_string(),
            interval: Duration::from_millis(10),
            count: Some(4),
            batch_size: Some(2),
        };
        let input = GenerateInput::new(config).unwrap();

        // 读取第一批消息 (2条)
        let result = input.read().await;
        assert!(result.is_ok());

        // 读取第二批消息 (2条，正好达到限制)
        let result = input.read().await;
        assert!(result.is_ok());

        // 尝试读取第三批消息 (应该返回Done错误)
        let result = input.read().await;
        assert!(matches!(result, Err(Error::Done)));
    }

    #[tokio::test]
    async fn test_deserialize_duration() {
        // 测试从JSON反序列化
        let json = r#"{
            "context": "test message",
            "interval": "10ms",
            "count": 5,
            "batch_size": 2
        }"#;

        let config: GenerateConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.context, "test message");
        assert_eq!(config.interval, Duration::from_millis(10));
        assert_eq!(config.count, Some(5));
        assert_eq!(config.batch_size, Some(2));
    }
}
