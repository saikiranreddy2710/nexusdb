//! MySQL wire protocol handler for NexusDB.
//!
//! Implements `AsyncMysqlShim` from the `opensrv-mysql` crate, enabling
//! standard MySQL clients (`mysql`, drivers, ORMs) to connect.

use std::io;
use std::sync::Arc;

use opensrv_mysql::*;
use tokio::io::AsyncWrite;
use tracing::{debug, error};

use nexus_sql::executor::Value;
use nexus_sql::parser::DataType;

use crate::database::{Database, StatementResult};

/// NexusDB MySQL protocol handler.
pub struct MysqlQueryHandler {
    db: Arc<Database>,
}

impl MysqlQueryHandler {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

/// Maps NexusDB DataType to MySQL ColumnType.
fn datatype_to_mysql_coltype(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::TinyInt => ColumnType::MYSQL_TYPE_TINY,
        DataType::SmallInt => ColumnType::MYSQL_TYPE_SHORT,
        DataType::Int => ColumnType::MYSQL_TYPE_LONG,
        DataType::BigInt => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Float => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Double => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::Decimal { .. } => ColumnType::MYSQL_TYPE_DECIMAL,
        DataType::Varchar(_) => ColumnType::MYSQL_TYPE_VARCHAR,
        DataType::Text => ColumnType::MYSQL_TYPE_STRING,
        DataType::Blob => ColumnType::MYSQL_TYPE_BLOB,
        DataType::Date => ColumnType::MYSQL_TYPE_DATE,
        DataType::Time => ColumnType::MYSQL_TYPE_TIME,
        DataType::Timestamp => ColumnType::MYSQL_TYPE_TIMESTAMP,
        _ => ColumnType::MYSQL_TYPE_STRING,
    }
}

/// Converts a NexusDB Value to a string for MySQL text protocol.
fn value_to_mysql_string(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::Int(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(f) => f.to_string(),
        Value::String(s) => s.clone(),
        _ => v.to_string(),
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for MysqlQueryHandler {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(42, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _stmt_id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default()).await
    }

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        debug!("MySQL query: {}", sql);

        let trimmed = sql.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() {
            return results.completed(OkResponse::default()).await;
        }

        // Create session, execute, close
        let session_id = self.db.create_session("nexusdb");
        let exec_result = {
            let session_arc = match self.db.get_session(session_id) {
                Some(s) => s,
                None => {
                    return results
                        .error(
                            ErrorKind::ER_INTERNAL_ERROR,
                            b"failed to create session",
                        )
                        .await;
                }
            };
            let mut session = session_arc.write().unwrap();
            session.execute(trimmed)
        };
        self.db.close_session(session_id);

        match exec_result {
            Ok(stmt_result) => write_mysql_result(stmt_result, results).await,
            Err(e) => {
                error!("MySQL query error: {}", e);
                results
                    .error(ErrorKind::ER_INTERNAL_ERROR, e.to_string().as_bytes())
                    .await
            }
        }
    }

    async fn on_init<'a>(
        &'a mut self,
        schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        debug!("MySQL USE {}", schema);
        let dbs = self.db.list_databases();
        if dbs.contains(&schema.to_string()) {
            writer.ok().await
        } else {
            writer
                .error(ErrorKind::ER_BAD_DB_ERROR, schema.as_bytes())
                .await
        }
    }

    async fn on_close(&mut self, _stmt_id: u32) {}
}

/// Writes a NexusDB statement result as MySQL wire protocol response.
async fn write_mysql_result<'a, W: AsyncWrite + Send + Unpin>(
    result: StatementResult,
    results: QueryResultWriter<'a, W>,
) -> io::Result<()> {
    match result {
        StatementResult::Query(exec_result) => {
            let cols: Vec<Column> = exec_result
                .schema
                .fields()
                .iter()
                .map(|f| Column {
                    table: String::new(),
                    column: f.name().to_string(),
                    coltype: datatype_to_mysql_coltype(&f.data_type),
                    colflags: ColumnFlags::empty(),
                })
                .collect();

            let mut writer = results.start(&cols).await?;

            for row in exec_result.rows() {
                for val in row.iter() {
                    writer.write_col(&value_to_mysql_string(val))?;
                }
                writer.end_row().await?;
            }

            writer.finish().await
        }
        StatementResult::Insert { rows_affected } => {
            results
                .completed(OkResponse {
                    affected_rows: rows_affected,
                    ..Default::default()
                })
                .await
        }
        StatementResult::Update { rows_affected } => {
            results
                .completed(OkResponse {
                    affected_rows: rows_affected,
                    ..Default::default()
                })
                .await
        }
        StatementResult::Delete { rows_affected } => {
            results
                .completed(OkResponse {
                    affected_rows: rows_affected,
                    ..Default::default()
                })
                .await
        }
        StatementResult::Ddl { .. }
        | StatementResult::Transaction { .. }
        | StatementResult::Empty => results.completed(OkResponse::default()).await,
    }
}
