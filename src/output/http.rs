//! HTTP output component
//!
//! Send the processed data to the HTTP endpoint

use crate::{output::Output, Error, MessageBatch};
use async_trait::async_trait;
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// HTTP output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpOutputConfig {
    /// Destination URL
    pub url: String,
    /// HTTP method
    pub method: String,
    /// Timeout Period (ms)
    pub timeout_ms: u64,
    /// Number of retries
    pub retry_count: u32,
    /// Request header
    pub headers: Option<std::collections::HashMap<String, String>>,
}

/// HTTP output component
pub struct HttpOutput {
    config: HttpOutputConfig,
    client: Arc<Mutex<Option<Client>>>,
    connected: AtomicBool,
}

impl HttpOutput {
    /// Create a new HTTP output component
    pub fn new(config: &HttpOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            client: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Output for HttpOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Create an HTTP client
        let client_builder =
            Client::builder().timeout(std::time::Duration::from_millis(self.config.timeout_ms));
        let client_arc = self.client.clone();
        client_arc.lock().await.replace(
            client_builder.build().map_err(|e| {
                Error::Connection(format!("Unable to create an HTTP client: {}", e))
            })?,
        );

        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        let client_arc = self.client.clone();
        let client_arc_guard = client_arc.lock().await;
        if !self.connected.load(Ordering::SeqCst) || client_arc_guard.is_none() {
            return Err(Error::Connection("The output is not connected".to_string()));
        }

        let client = client_arc_guard.as_ref().unwrap();
        let content = msg.as_string()?;
        if content.is_empty() {
            return Ok(());
        }
        let body;
        if content.len() == 1 {
            body = content[0].clone();
        } else {
            body = serde_json::to_string(&content)
                .map_err(|_| Error::Processing("Unable to serialize message".to_string()))?;
        }

        // Build the request
        let mut request_builder = match self.config.method.to_uppercase().as_str() {
            "GET" => client.get(&self.config.url),
            "POST" => client.post(&self.config.url).body(body), // Content-Type由统一逻辑添加
            "PUT" => client.put(&self.config.url).body(body),
            "DELETE" => client.delete(&self.config.url),
            "PATCH" => client.patch(&self.config.url).body(body),
            _ => {
                return Err(Error::Config(format!(
                    "HTTP methods that are not supported: {}",
                    self.config.method
                )))
            }
        };

        // Add request headers
        if let Some(headers) = &self.config.headers {
            for (key, value) in headers {
                request_builder = request_builder.header(key, value);
            }
        }

        // Add content type header (if not specified)
        // 始终添加Content-Type头（如果未指定）
        if let Some(headers) = &self.config.headers {
            if !headers.contains_key("Content-Type") {
                request_builder = request_builder.header(header::CONTENT_TYPE, "application/json");
            }
        } else {
            request_builder = request_builder.header(header::CONTENT_TYPE, "application/json");
        }

        // Send a request
        let mut retry_count = 0;
        let mut last_error = None;

        while retry_count <= self.config.retry_count {
            match request_builder.try_clone().unwrap().send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(());
                    } else {
                        let status = response.status();
                        let body = response
                            .text()
                            .await
                            .unwrap_or_else(|_| "<Unable to read response body>".to_string());
                        last_error = Some(Error::Processing(format!(
                            "HTTP Request Failed: Status code {}, response: {}",
                            status, body
                        )));
                    }
                }
                Err(e) => {
                    last_error = Some(Error::Connection(format!("HTTP request error: {}", e)));
                }
            }

            retry_count += 1;
            if retry_count <= self.config.retry_count {
                // Index backoff retry
                tokio::time::sleep(std::time::Duration::from_millis(
                    100 * 2u64.pow(retry_count - 1),
                ))
                .await;
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Unknown("Unknown HTTP error".to_string())))
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        let mut guard = self.client.lock().await;
        *guard = None;
        Ok(())
    }
}
 