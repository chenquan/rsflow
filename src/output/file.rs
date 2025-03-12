//! File output component
//!
//! Output the processed data to a file

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::{output::Output, Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// File output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOutputConfig {
    /// Output file path
    pub path: String,
    /// Whether to add a newline after each message
    pub append_newline: Option<bool>,
    /// Whether to append to the end of the file (instead of overwriting)
    pub append: Option<bool>,
}

/// File output component
pub struct FileOutput {
    config: FileOutputConfig,
    writer: Arc<Mutex<Option<File>>>,
    connected: AtomicBool,
}

impl FileOutput {
    /// Create a new file output component
    pub fn new(config: &FileOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            writer: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Output for FileOutput {
    async fn connect(&self) -> Result<(), Error> {
        let path = Path::new(&self.config.path);

        // Make sure the directory exists
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(Error::Io)?
            }
        }
        let append = self.config.append.unwrap_or(true);
        // Open the file
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(append)
            .truncate(!append)
            .open(path)
            .map_err(Error::Io)?;
        let writer_arc = self.writer.clone();
        let mut writer_arc_guard = writer_arc.lock().await;
        writer_arc_guard.replace(file);
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        let writer_arc = self.writer.clone();
        let writer_arc_guard = writer_arc.lock().await;
        if !self.connected.load(Ordering::SeqCst) || writer_arc_guard.is_none() {
            return Err(Error::Connection("The output is not connected".to_string()));
        }

        let content = msg.as_string()?;
        let writer = writer_arc_guard.as_ref();
        let mut file =
            writer.ok_or(Error::Connection("The output is not connected".to_string()))?;

        for x in content {
            if self.config.append_newline.unwrap_or(true) {
                writeln!(file, "{}", x).map_err(Error::Io)?
            } else {
                write!(file, "{}", x).map_err(Error::Io)?
            }
        }

        file.flush().map_err(Error::Io)?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        let writer_arc = self.writer.clone();
        let mut writer_arc_mutex_guard = writer_arc.lock().await;
        *writer_arc_mutex_guard = None;
        Ok(())
    }
}
