//! MQTT output component
//!
//! Send the processed data to the MQTT broker

use crate::{output::Output, Error, MessageBatch};
use async_trait::async_trait;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// MQTT output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttOutputConfig {
    /// MQTT broker address
    pub host: String,
    /// MQTT broker port
    pub port: u16,
    /// Client ID
    pub client_id: String,
    /// Username (optional)
    pub username: Option<String>,
    /// Password (optional)
    pub password: Option<String>,
    /// Published topics
    pub topic: String,
    /// Quality of Service (0, 1, 2)
    pub qos: Option<u8>,
    /// Whether to use clean session
    pub clean_session: Option<bool>,
    /// Keep alive interval (seconds)
    pub keep_alive: Option<u64>,
    /// Whether to retain the message
    pub retain: Option<bool>,
}

/// MQTT output component
pub struct MqttOutput {
    config: MqttOutputConfig,
    client: Arc<Mutex<Option<AsyncClient>>>,
    connected: AtomicBool,
    eventloop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl MqttOutput {
    /// Create a new MQTT output component
    pub fn new(config: &MqttOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            client: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            eventloop_handle: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl Output for MqttOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Create MQTT options
        let mut mqtt_options =
            MqttOptions::new(&self.config.client_id, &self.config.host, self.config.port);

        // Set the authentication information
        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            mqtt_options.set_credentials(username, password);
        }

        // Set the keep-alive time
        if let Some(keep_alive) = self.config.keep_alive {
            mqtt_options.set_keep_alive(std::time::Duration::from_secs(keep_alive));
        }

        // Set up a purge session
        if let Some(clean_session) = self.config.clean_session {
            mqtt_options.set_clean_session(clean_session);
        }

        // Create an MQTT client
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

        // Save the client
        let client_arc = self.client.clone();
        let mut client_guard = client_arc.lock().await;
        *client_guard = Some(client);

        // Start an event loop processing thread (keep the connection active)
        let eventloop_handle = tokio::spawn(async move {
            while let Ok(_) = eventloop.poll().await {
                // Just keep the event loop running and don't need to process the event
            }
        });

        // Holds the event loop processing thread handle
        let eventloop_handle_arc = self.eventloop_handle.clone();
        let mut eventloop_handle_guard = eventloop_handle_arc.lock().await;
        *eventloop_handle_guard = Some(eventloop_handle);

        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection("The output is not connected".to_string()));
        }

        let client_arc = self.client.clone();
        let client_guard = client_arc.lock().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Connection("The MQTT client is not initialized".to_string()))?;

        // Get the message content
        let payloads = match msg.as_string() {
            Ok(v) => v.to_vec(),
            Err(e) => {
                return Err(e);
            }
        };

        for payload in payloads {
            info!(
                "Send message: {}",
                &String::from_utf8_lossy((&payload).as_ref())
            );

            // Determine the QoS level
            let qos_level = match self.config.qos {
                Some(0) => QoS::AtMostOnce,
                Some(1) => QoS::AtLeastOnce,
                Some(2) => QoS::ExactlyOnce,
                _ => QoS::AtLeastOnce, // The default is QoS 1
            };

            // Decide whether to keep the message
            let retain = self.config.retain.unwrap_or(false);

            // Post a message
            client
                .publish(&self.config.topic, qos_level, retain, payload)
                .await
                .map_err(|e| Error::Processing(format!("MQTT publishing failed: {}", e)))?;
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // Stop the event loop processing thread
        let mut eventloop_handle_guard = self.eventloop_handle.lock().await;
        if let Some(handle) = eventloop_handle_guard.take() {
            handle.abort();
        }

        // Disconnect the MQTT connection
        let client_arc = self.client.clone();
        let client_guard = client_arc.lock().await;
        if let Some(client) = &*client_guard {
            // Try to disconnect, but don't wait for the result
            let _ = client.disconnect().await;
        }

        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}
