//! Memory input component
//!
//! Read data from an in-memory message queue

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::input::Ack;
use crate::input::NoopAck;
use crate::{input::Input, Error, MessageBatch};

/// Memory input configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInputConfig {
    /// Initial message for the memory queue
    pub messages: Option<Vec<String>>,
}

/// Memory input component
pub struct MemoryInput {
    queue: Arc<Mutex<VecDeque<MessageBatch>>>,
    connected: AtomicBool,
}

impl MemoryInput {
    /// Create a new memory input component
    pub fn new(config: &MemoryInputConfig) -> Result<Self, Error> {
        let mut queue = VecDeque::new();

        // If there is an initial message in the configuration, it is added to the queue
        if let Some(messages) = &config.messages {
            for msg_str in messages {
                queue.push_back(MessageBatch::from_string(msg_str));
            }
        }

        Ok(Self {
            queue: Arc::new(Mutex::new(queue)),
            connected: AtomicBool::new(false),
        })
    }

    /// Add a message to the memory input
    pub async fn push(&self, msg: MessageBatch) -> Result<(), Error> {
        let mut queue = self.queue.lock().await;
        queue.push_back(msg);
        Ok(())
    }
}

#[async_trait]
impl Input for MemoryInput {
    async fn connect(&self) -> Result<(), Error> {
        self.connected
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        if !self.connected.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Connection("The input is not connected".to_string()));
        }

        // 尝试从队列中获取消息
        let msg_option;
        {
            let mut queue = self.queue.lock().await;
            msg_option = queue.pop_front();
        }

        if let Some(msg) = msg_option {
            Ok((msg, Arc::new(NoopAck)))
        } else {
            Err(Error::Done)
        }
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected
            .store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_input_new() {
        // 测试创建MemoryInput实例，不带初始消息
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(&config);
        assert!(input.is_ok());

        // 测试创建MemoryInput实例，带初始消息
        let messages = vec!["message1".to_string(), "message2".to_string()];
        let config = MemoryInputConfig {
            messages: Some(messages),
        };
        let input = MemoryInput::new(&config);
        assert!(input.is_ok());
    }

    #[tokio::test]
    async fn test_memory_input_connect() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(&config).unwrap();

        // 测试连接
        let result = input.connect().await;
        assert!(result.is_ok());

        // 验证连接状态
        assert!(input.connected.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_memory_input_read_without_connect() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(&config).unwrap();

        // 未连接时读取应该返回错误
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(_)) => {} // 期望的错误类型
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_read_empty_queue() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(&config).unwrap();

        // 连接
        assert!(input.connect().await.is_ok());

        // 队列为空，应该返回Done错误
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Done) => {} // 期望的错误类型
            _ => panic!("Expected Done error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_read_with_initial_messages() {
        let messages = vec!["message1".to_string(), "message2".to_string()];
        let config = MemoryInputConfig {
            messages: Some(messages),
        };
        let input = MemoryInput::new(&config).unwrap();

        // 连接
        assert!(input.connect().await.is_ok());

        // 读取第一条消息
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message1"]);
        ack.ack().await;

        // 读取第二条消息
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message2"]);
        ack.ack().await;

        // 队列为空，应该返回Done错误
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Done) => {} // 期望的错误类型
            _ => panic!("Expected Done error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_push() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(&config).unwrap();

        // 连接
        assert!(input.connect().await.is_ok());

        // 推送消息
        let msg = MessageBatch::from_string("pushed message");
        assert!(input.push(msg).await.is_ok());

        // 读取推送的消息
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["pushed message"]);
        ack.ack().await;

        // 队列为空，应该返回Done错误
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Done) => {} // 期望的错误类型
            _ => panic!("Expected Done error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_close() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(&config).unwrap();

        // 连接
        assert!(input.connect().await.is_ok());
        assert!(input.connected.load(std::sync::atomic::Ordering::SeqCst));

        // 关闭
        assert!(input.close().await.is_ok());
        assert!(!input.connected.load(std::sync::atomic::Ordering::SeqCst));

        // 关闭后读取应该返回错误
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(_)) => {} // 期望的错误类型
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_memory_input_multiple_push_read() {
        let config = MemoryInputConfig { messages: None };
        let input = MemoryInput::new(&config).unwrap();

        // 连接
        assert!(input.connect().await.is_ok());

        // 推送多条消息
        let msg1 = MessageBatch::from_string("message1");
        let msg2 = MessageBatch::from_string("message2");
        let msg3 = MessageBatch::from_string("message3");

        assert!(input.push(msg1).await.is_ok());
        assert!(input.push(msg2).await.is_ok());
        assert!(input.push(msg3).await.is_ok());

        // 按顺序读取消息
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message1"]);
        ack.ack().await;

        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message2"]);
        ack.ack().await;

        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["message3"]);
        ack.ack().await;

        // 队列为空，应该返回Done错误
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Done) => {} // 期望的错误类型
            _ => panic!("Expected Done error"),
        }
    }
}
