//! SQL处理器组件
//!
//! 使用DataFusion执行SQL查询处理数据，支持静态SQL和流式SQL

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
/// SQL处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProcessorConfig {
    /// SQL查询语句
    pub query: String,

    /// 表名（用于SQL查询中引用）
    pub table_name: Option<String>,
}

/// SQL处理器组件
pub struct SqlProcessor {
    config: SqlProcessorConfig,
}

impl SqlProcessor {
    /// 创建一个新的SQL处理器组件
    pub fn new(config: &SqlProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
        })
    }

    /// 执行SQL查询
    async fn execute_query(&self, batch: RecordBatch) -> Result<RecordBatch, Error> {
        // 创建会话上下文
        let ctx = SessionContext::new();

        // 注册表
        let table_name = self
            .config
            .table_name
            .as_deref()
            .unwrap_or(DEFAULT_TABLE_NAME);

        ctx.register_batch(table_name, batch)
            .map_err(|e| Error::Processing(format!("注册表失败: {}", e)))?;

        // 执行SQL查询并收集结果
        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = ctx
            .sql_with_options(&self.config.query, sql_options)
            .await
            .map_err(|e| Error::Processing(format!("SQL查询错误: {}", e)))?;

        let result_batches = df
            .collect()
            .await
            .map_err(|e| Error::Processing(format!("收集查询结果错误: {}", e)))?;

        if result_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        if result_batches.len() == 1 {
            return Ok(result_batches[0].clone());
        }

        Ok(
            arrow::compute::concat_batches(&&result_batches[0].schema(), &result_batches)
                .map_err(|e| Error::Processing(format!("合并批次失败: {}", e)))?,
        )
    }
}

#[async_trait]
impl Processor for SqlProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // 如果批次为空，直接返回空结果
        if msg_batch.is_empty() {
            return Ok(vec![]);
        }

        let batch: RecordBatch = match msg_batch.content {
            Content::Arrow(v) => v,
            Content::Binary(_) => {
                return Err(Error::Processing("不支持的输入格式".to_string()))?;
            }
        };

        // 执行SQL查询
        let result_batch = self.execute_query(batch).await?;
        Ok(vec![MessageBatch::new_arrow(result_batch)])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
