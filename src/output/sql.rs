use crate::input::Input;
use crate::output::Output;
use crate::{Content, Error, MessageBatch};
use async_trait::async_trait;
use datafusion::prelude::{SQLOptions, SessionContext};
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlConfig {
    select_sql: String,
    create_table_sql: String,
}
pub struct SqlOutput {
    sql_config: SqlConfig,
    read: AtomicBool,
    ctx: Arc<RwLock<Option<SessionContext>>>,
}

#[async_trait]
impl Output for SqlOutput {
    async fn connect(&self) -> Result<(), Error> {
        let ctx_arc = self.ctx.clone();
        let mut ctx_guard = ctx_arc.write().await;

        let ctx = SessionContext::new();

        let sql_options = SQLOptions::new()
            .with_allow_ddl(true)
            .with_allow_dml(false)
            .with_allow_statements(false);
        ctx.sql_with_options(&self.sql_config.create_table_sql, sql_options)
            .await
            .map_err(|e| Error::Connection(format!("Failed to execute SQL query: {}", e)))?;
        ctx_guard.replace(ctx);

        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        let x = match &msg.content {
            Content::Arrow(v) => v,
            Content::Binary(_) => {
                return Err(Error::Connection(
                    "SQL output does not support binary messages".to_string(),
                ));
            }
        };

        if x.num_rows() == 0 {
            return Ok(());
        }

        let ctx_arc = self.ctx.clone();

        let ctx_guard = ctx_arc.read().await;
        if ctx_guard.is_none() {
            return Err(Error::Connection(
                "SQL context is not initialized".to_string(),
            ));
        };
        let ctx = ctx_guard.as_ref().unwrap();
        let df = ctx
            .sql(&self.sql_config.select_sql)
            .await
            .map_err(|e| Error::Connection(format!("Failed to execute SQL query: {}", e)))?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        let ctx_arc = self.ctx.clone();
        let mut ctx_arc_guard = ctx_arc.write().await;
        ctx_arc_guard.take();
        Ok(())
    }
}
