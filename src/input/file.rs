//! File Input Component
//!
//! Read data from the file system

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::input::{Ack, NoopAck};
use crate::{input::Input, Error, MessageBatch};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInputConfig {
    /// 输入文件路径
    pub path: String,
    /// 是否在读取完成后关闭
    pub close_on_eof: Option<bool>,
    /// 是否从文件开头开始读取（否则从末尾开始）
    pub start_from_beginning: Option<bool>,
}

/// 文件输入组件
pub struct FileInput {
    config: FileInputConfig,
    reader: Arc<Mutex<Option<BufReader<File>>>>,
    connected: AtomicBool,
    eof_reached: AtomicBool,
}

impl FileInput {
    /// 创建一个新的文件输入组件
    pub fn new(config: &FileInputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            reader: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            eof_reached: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Input for FileInput {
    async fn connect(&self) -> Result<(), Error> {
        let path = Path::new(&self.config.path);

        // Open the file
        let file = File::open(path)
            .map_err(|e| Error::Connection(format!("无法打开文件 {}: {}", self.config.path, e)))?;

        let mut reader = BufReader::new(file);

        // If it is not read from the beginning, it moves to the end of the file
        if !self.config.start_from_beginning.unwrap_or(true) {
            reader
                .seek(SeekFrom::End(0))
                .map_err(|e| Error::Processing(format!("无法定位到文件末尾: {}", e)))?;
        }

        let reader_arc = self.reader.clone();
        reader_arc.lock().await.replace(reader);
        self.connected.store(true, Ordering::SeqCst);
        self.eof_reached.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let reader_arc = self.reader.clone();
        let mut reader_mutex = reader_arc.lock().await;
        if !self.connected.load(Ordering::SeqCst) || reader_mutex.is_none() {
            return Err(Error::Connection("The input is not connected".to_string()));
        }

        if self.eof_reached.load(Ordering::SeqCst) && self.config.close_on_eof.unwrap_or(true) {
            return Err(Error::Done);
        }

        let bytes_read;
        let mut line = String::new();
        {
            let reader_mutex = reader_mutex.as_mut();
            if reader_mutex.is_none() {
                return Err(Error::Connection("The input is not connected".to_string()));
            }

            let reader = reader_mutex.unwrap();
            bytes_read = reader.read_line(&mut line).map_err(Error::Io)?;
        }

        if bytes_read == 0 {
            self.eof_reached.store(true, Ordering::SeqCst);

            if self.config.close_on_eof.unwrap_or(true) {
                return Err(Error::Done);
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            return Err(Error::Processing("Wait for new data".to_string()));
        }

        // Remove the trailing line break
        if line.ends_with('\n') {
            line.pop();
            if line.ends_with('\r') {
                line.pop();
            }
        }

        Ok((MessageBatch::from_string(&line), Arc::new(NoopAck)))
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        let reader_arc = self.reader.clone();
        let mut reader_mutex = reader_arc.lock().await;
        *reader_mutex = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_input_new() {
        let config = FileInputConfig {
            path: "test.txt".to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(&config);
        assert!(input.is_ok());
    }

    #[tokio::test]
    async fn test_file_input_connect_file_not_exists() {
        let config = FileInputConfig {
            path: "non_existent_file.txt".to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(&config).unwrap();
        let result = input.connect().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(_)) => {} // 期望的错误类型
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_file_input_read_without_connect() {
        let config = FileInputConfig {
            path: "test.txt".to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(&config).unwrap();
        let result = input.read().await;
        assert!(result.is_err());
        match result {
            Err(Error::Connection(_)) => {} // 期望的错误类型
            _ => panic!("Expected Connection error"),
        }
    }

    #[tokio::test]
    async fn test_file_input_read_from_beginning() {
        // 创建临时目录和文件
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let file_path_str = file_path.to_str().unwrap();

        // 写入测试数据
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "line1").unwrap();
        writeln!(file, "line2").unwrap();
        writeln!(file, "line3").unwrap();
        file.flush().unwrap();

        // 配置从文件开头读取
        let config = FileInputConfig {
            path: file_path_str.to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(&config).unwrap();

        // 连接并读取
        assert!(input.connect().await.is_ok());

        // 读取第一行
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line1"]);
        ack.ack().await;

        // 读取第二行
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line2"]);
        ack.ack().await;

        // 读取第三行
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line3"]);
        ack.ack().await;

        // 文件结束，应返回Done错误
        let result = input.read().await;
        assert!(matches!(result, Err(Error::Done)));

        // 关闭连接
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_file_input_read_from_end() {
        // 创建临时目录和文件
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let file_path_str = file_path.to_str().unwrap();

        // 写入测试数据
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "line1").unwrap();
        writeln!(file, "line2").unwrap();
        file.flush().unwrap();

        // 配置从文件末尾读取
        let config = FileInputConfig {
            path: file_path_str.to_string(),
            close_on_eof: Some(true),
            start_from_beginning: Some(false),
        };
        let input = FileInput::new(&config).unwrap();

        // 连接
        assert!(input.connect().await.is_ok());

        // 从末尾读取，应该没有数据，返回Done错误
        let result = input.read().await;
        assert!(matches!(result, Err(Error::Done)));

        // 追加新数据
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&file_path)
            .unwrap();
        writeln!(file, "line3").unwrap();
        file.flush().unwrap();

        // 重新连接
        assert!(input.close().await.is_ok());
        assert!(input.connect().await.is_ok());

        // 现在应该能读取到新添加的行
        let result = input.read().await;
        assert!(matches!(result, Err(Error::Done))); // 仍然是从末尾读取，所以没有数据

        // 关闭连接
        assert!(input.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_file_input_close_on_eof_false() {
        // 创建临时目录和文件
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let file_path_str = file_path.to_str().unwrap();

        // 写入测试数据
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "line1").unwrap();
        file.flush().unwrap();

        // 配置读取完成后不关闭
        let config = FileInputConfig {
            path: file_path_str.to_string(),
            close_on_eof: Some(false),
            start_from_beginning: Some(true),
        };
        let input = FileInput::new(&config).unwrap();

        // 连接并读取
        assert!(input.connect().await.is_ok());

        // 读取第一行
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line1"]);
        ack.ack().await;

        // 文件结束，但不关闭，应返回Processing错误
        let result = input.read().await;
        assert!(matches!(result, Err(Error::Processing(_))));

        // 追加新数据
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&file_path)
            .unwrap();
        writeln!(file, "line2").unwrap();
        file.flush().unwrap();

        // 应该能读取到新添加的行
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await; // 等待一段时间确保文件写入完成
        let (batch, ack) = input.read().await.unwrap();
        assert_eq!(batch.as_string().unwrap(), vec!["line2"]);
        ack.ack().await;

        // 关闭连接
        assert!(input.close().await.is_ok());
    }
}
