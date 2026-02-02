//! gRPC service implementation for NexusDB.
//!
//! This module implements the NexusDB gRPC service defined in nexus-proto.

use std::sync::Arc;
use std::time::Instant;

use tonic::{Request, Response, Status};

use nexus_proto::proto::{
    nexus_db_server::NexusDb, BeginTransactionRequest, BeginTransactionResponse, ColumnInfo,
    CommandResult, CommandType, CommitRequest, CommitResponse, DataType, ErrorCode,
    ExecutePreparedRequest, ExecuteRequest, ExecuteResponse, ExecutionStats, PingRequest,
    PingResponse, PrepareRequest, PrepareResponse, QueryResult, RollbackRequest, RollbackResponse,
    Row, RowBatch, ServerInfoRequest, ServerInfoResponse, Value,
};

use crate::database::{Database, ExecuteResult, StatementResult};

/// NexusDB gRPC service implementation.
pub struct NexusDbService {
    /// The database instance.
    db: Arc<Database>,
    /// Server start time.
    start_time: Instant,
}

impl NexusDbService {
    /// Creates a new gRPC service with the given database.
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            start_time: Instant::now(),
        }
    }

    /// Converts an ExecuteResult to a QueryResult proto.
    fn execute_result_to_query_result(&self, result: &ExecuteResult) -> QueryResult {
        let fields = result.schema.fields();
        
        let proto_columns: Vec<ColumnInfo> = fields
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().to_string(),
                data_type: DataType::String as i32, // Simplified
                nullable: f.nullable,
                table_name: None,
            })
            .collect();

        let proto_rows: Vec<Row> = result.rows()
            .into_iter()
            .map(|row| {
                let values: Vec<Value> = row
                    .iter()
                    .map(|v| {
                        use nexus_proto::proto::value::Value as ProtoValue;
                        Value {
                            value: Some(ProtoValue::StringValue(v.to_string())),
                        }
                    })
                    .collect();
                Row { values }
            })
            .collect();

        QueryResult {
            columns: proto_columns,
            rows: proto_rows,
            has_more: false,
        }
    }

    /// Converts a StatementResult to an ExecuteResponse.
    fn statement_to_response(
        &self,
        result: StatementResult,
        stats: ExecutionStats,
    ) -> ExecuteResponse {
        use nexus_proto::proto::execute_response::Result;

        match result {
            StatementResult::Query(exec_result) => {
                let query_result = self.execute_result_to_query_result(&exec_result);
                ExecuteResponse {
                    success: true,
                    error: None,
                    error_code: None,
                    result: Some(Result::QueryResult(query_result)),
                    stats: Some(stats),
                }
            }
            StatementResult::Ddl { command } => ExecuteResponse {
                success: true,
                error: None,
                error_code: None,
                result: Some(Result::CommandResult(CommandResult {
                    command_type: match command.to_uppercase().as_str() {
                        s if s.starts_with("CREATE TABLE") => CommandType::CreateTable as i32,
                        s if s.starts_with("DROP TABLE") => CommandType::DropTable as i32,
                        s if s.starts_with("CREATE INDEX") => CommandType::CreateIndex as i32,
                        s if s.starts_with("DROP INDEX") => CommandType::DropIndex as i32,
                        _ => CommandType::Unknown as i32,
                    },
                    rows_affected: 0,
                    message: Some(command),
                })),
                stats: Some(stats),
            },
            StatementResult::Insert { rows_affected } => ExecuteResponse {
                success: true,
                error: None,
                error_code: None,
                result: Some(Result::CommandResult(CommandResult {
                    command_type: CommandType::Insert as i32,
                    rows_affected,
                    message: None,
                })),
                stats: Some(stats),
            },
            StatementResult::Update { rows_affected } => ExecuteResponse {
                success: true,
                error: None,
                error_code: None,
                result: Some(Result::CommandResult(CommandResult {
                    command_type: CommandType::Update as i32,
                    rows_affected,
                    message: None,
                })),
                stats: Some(stats),
            },
            StatementResult::Delete { rows_affected } => ExecuteResponse {
                success: true,
                error: None,
                error_code: None,
                result: Some(Result::CommandResult(CommandResult {
                    command_type: CommandType::Delete as i32,
                    rows_affected,
                    message: None,
                })),
                stats: Some(stats),
            },
            StatementResult::Transaction { command } => ExecuteResponse {
                success: true,
                error: None,
                error_code: None,
                result: Some(Result::CommandResult(CommandResult {
                    command_type: match command.to_uppercase().as_str() {
                        "BEGIN" => CommandType::Begin as i32,
                        "COMMIT" => CommandType::Commit as i32,
                        "ROLLBACK" => CommandType::Rollback as i32,
                        _ => CommandType::Unknown as i32,
                    },
                    rows_affected: 0,
                    message: Some(command),
                })),
                stats: Some(stats),
            },
            StatementResult::Empty => ExecuteResponse {
                success: true,
                error: None,
                error_code: None,
                result: None,
                stats: Some(stats),
            },
        }
    }

    /// Creates an error response.
    fn error_response(&self, message: String, code: ErrorCode) -> ExecuteResponse {
        ExecuteResponse {
            success: false,
            error: Some(message),
            error_code: Some(code as i32),
            result: None,
            stats: None,
        }
    }
}

#[tonic::async_trait]
impl NexusDb for NexusDbService {
    /// Execute a SQL statement.
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let req = request.into_inner();
        let start = Instant::now();

        // Execute the SQL
        match self.db.execute(&req.sql) {
            Ok(result) => {
                let elapsed = start.elapsed();
                let stats = ExecutionStats {
                    parse_time_us: 0,
                    plan_time_us: 0,
                    execute_time_us: elapsed.as_micros() as u64,
                    total_time_us: elapsed.as_micros() as u64,
                    rows_scanned: 0,
                    rows_returned: result.rows_affected().unwrap_or(0),
                    bytes_read: 0,
                    was_cached: false,
                };

                Ok(Response::new(self.statement_to_response(result, stats)))
            }
            Err(e) => {
                let error_code = match &e {
                    crate::database::DatabaseError::ParseError(_) => ErrorCode::SyntaxError,
                    crate::database::DatabaseError::PlanError(_) => ErrorCode::InvalidQuery,
                    crate::database::DatabaseError::ExecutionError(_) => ErrorCode::InternalError,
                    crate::database::DatabaseError::TransactionError(_) => {
                        ErrorCode::TransactionNotFound
                    }
                    _ => ErrorCode::InternalError,
                };

                Ok(Response::new(
                    self.error_response(e.to_string(), error_code),
                ))
            }
        }
    }

    /// Execute a streaming query (for large result sets).
    type ExecuteStreamStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<RowBatch, Status>> + Send>>;

    async fn execute_stream(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStreamStream>, Status> {
        let req = request.into_inner();

        // Execute the query
        let result = self.db.execute(&req.sql).map_err(|e| {
            Status::internal(format!("Query execution failed: {}", e))
        })?;

        // Convert to a stream
        let rows: Vec<Row> = match result {
            StatementResult::Query(exec_result) => {
                exec_result.rows()
                    .into_iter()
                    .map(|row| {
                        let values: Vec<Value> = row
                            .iter()
                            .map(|v| {
                                use nexus_proto::proto::value::Value as ProtoValue;
                                Value {
                                    value: Some(ProtoValue::StringValue(v.to_string())),
                                }
                            })
                            .collect();
                        Row { values }
                    })
                    .collect()
            }
            _ => vec![],
        };

        // Create a stream that yields row batches
        let batch_size = 100;
        let row_count = rows.len();
        let batches: Vec<RowBatch> = rows
            .chunks(batch_size)
            .enumerate()
            .map(|(i, chunk)| {
                let is_last = (i + 1) * batch_size >= row_count;
                RowBatch {
                    rows: chunk.to_vec(),
                    is_last,
                }
            })
            .collect();

        // If no batches, send an empty final batch
        let batches = if batches.is_empty() {
            vec![RowBatch {
                rows: vec![],
                is_last: true,
            }]
        } else {
            batches
        };

        let stream = futures_util::stream::iter(batches.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    /// Prepare a statement for later execution.
    async fn prepare(
        &self,
        _request: Request<PrepareRequest>,
    ) -> Result<Response<PrepareResponse>, Status> {
        // For now, return a placeholder - prepared statements not yet implemented
        Ok(Response::new(PrepareResponse {
            success: false,
            error: Some("Prepared statements not yet implemented".to_string()),
            statement_id: 0,
            parameters: vec![],
            columns: vec![],
        }))
    }

    /// Execute a prepared statement.
    async fn execute_prepared(
        &self,
        _request: Request<ExecutePreparedRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        Ok(Response::new(self.error_response(
            "Prepared statements not yet implemented".to_string(),
            ErrorCode::InternalError,
        )))
    }

    /// Begin a new transaction.
    async fn begin_transaction(
        &self,
        _request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        // For now, we can't easily expose session-level transactions via stateless gRPC
        // In a real implementation, we'd need session affinity or transaction tokens
        
        // Execute BEGIN and return a transaction ID
        match self.db.execute("BEGIN") {
            Ok(_) => {
                // Generate a pseudo transaction ID (in real impl, would come from session)
                let txn_id = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;

                Ok(Response::new(BeginTransactionResponse {
                    success: true,
                    error: None,
                    transaction_id: txn_id,
                }))
            }
            Err(e) => Ok(Response::new(BeginTransactionResponse {
                success: false,
                error: Some(e.to_string()),
                transaction_id: 0,
            })),
        }
    }

    /// Commit a transaction.
    async fn commit(
        &self,
        _request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        match self.db.execute("COMMIT") {
            Ok(_) => Ok(Response::new(CommitResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(CommitResponse {
                success: false,
                error: Some(e.to_string()),
            })),
        }
    }

    /// Rollback a transaction.
    async fn rollback(
        &self,
        _request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        match self.db.execute("ROLLBACK") {
            Ok(_) => Ok(Response::new(RollbackResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(RollbackResponse {
                success: false,
                error: Some(e.to_string()),
            })),
        }
    }

    /// Ping the server.
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        let server_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Ok(Response::new(PingResponse {
            payload: req.payload,
            server_time_ms: server_time,
        }))
    }

    /// Get server information.
    async fn get_server_info(
        &self,
        _request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        let stats = self.db.stats();
        let uptime = self.start_time.elapsed();

        Ok(Response::new(ServerInfoResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            server_name: "NexusDB".to_string(),
            protocol_version: 1,
            features: vec![
                "SQL".to_string(),
                "Transactions".to_string(),
                "MVCC".to_string(),
                "Streaming".to_string(),
            ],
            uptime_seconds: uptime.as_secs(),
            active_connections: stats.active_sessions as u32,
        }))
    }
}

/// gRPC server builder.
pub struct GrpcServer {
    /// Database instance.
    db: Arc<Database>,
    /// Server address.
    addr: std::net::SocketAddr,
}

impl GrpcServer {
    /// Creates a new gRPC server builder.
    pub fn new(db: Arc<Database>, addr: std::net::SocketAddr) -> Self {
        Self { db, addr }
    }

    /// Starts the gRPC server.
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let service = NexusDbService::new(self.db);

        tracing::info!("NexusDB gRPC server listening on {}", self.addr);

        tonic::transport::Server::builder()
            .add_service(nexus_proto::NexusDbServer::new(service))
            .serve(self.addr)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_creation() {
        let db = Database::open_memory().unwrap();
        let _service = NexusDbService::new(Arc::new(db));
    }
}
