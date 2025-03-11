//! SQL processor component
//!
//! DataFusion is used to process data with SQL queries.

use crate::processor::Processor;
use crate::{Content, Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const DEFAULT_TABLE_NAME: &str = "flow";
/// SQL processor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProcessorConfig {
    /// SQL query statement
    pub query: String,

    /// Table name (used in SQL queries)
    pub table_name: Option<String>,
}

/// SQL processor component
pub struct SqlProcessor {
    config: SqlProcessorConfig,
}

impl SqlProcessor {
    /// Create a new SQL processor component.
    pub fn new(config: &SqlProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
        })
    }

    /// Execute SQL query
    async fn execute_query(&self, batch: RecordBatch) -> Result<RecordBatch, Error> {
        // Create a session context
        let ctx = SessionContext::new();
        let table_name = self
            .config
            .table_name
            .as_deref()
            .unwrap_or(DEFAULT_TABLE_NAME);

        ctx.register_batch(table_name, batch)
            .map_err(|e| Error::Processing(format!("Registration failed: {}", e)))?;

        // Execute the SQL query and collect the results.
        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = ctx
            .sql_with_options(&self.config.query, sql_options)
            .await
            .map_err(|e| Error::Processing(format!("SQL query error: {}", e)))?;

        let result_batches = df
            .collect()
            .await
            .map_err(|e| Error::Processing(format!("Collection query results error: {}", e)))?;

        if result_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        if result_batches.len() == 1 {
            return Ok(result_batches[0].clone());
        }

        Ok(
            arrow::compute::concat_batches(&&result_batches[0].schema(), &result_batches)
                .map_err(|e| Error::Processing(format!("Batch merge failed: {}", e)))?,
        )
    }
}

#[async_trait]
impl Processor for SqlProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // If the batch is empty, return an empty result.
        if msg_batch.is_empty() {
            return Ok(vec![]);
        }

        let batch: RecordBatch = match msg_batch.content {
            Content::Arrow(v) => v,
            Content::Binary(_) => {
                return Err(Error::Processing("Unsupported input format".to_string()))?;
            }
        };

        // Execute SQL query
        let result_batch = self.execute_query(batch).await?;
        Ok(vec![MessageBatch::new_arrow(result_batch)])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
