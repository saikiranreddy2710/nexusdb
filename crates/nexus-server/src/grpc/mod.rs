//! gRPC service implementation for NexusDB.
//!
//! This module implements the NexusDB gRPC service defined in nexus-proto.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use tonic::{Request, Response, Status};

use nexus_proto::proto::{
    nexus_db_server::NexusDb, BeginTransactionRequest, BeginTransactionResponse, ColumnInfo,
    CommandResult, CommandType, CommitRequest, CommitResponse, DataType, ErrorCode,
    ExecutePreparedRequest, ExecuteRequest, ExecuteResponse, ExecutionStats, PingRequest,
    PingResponse, PrepareRequest, PrepareResponse, QueryResult, RollbackRequest, RollbackResponse,
    Row, RowBatch, ServerInfoRequest, ServerInfoResponse, Value,
};

use nexus_security::{Credential, Identity};

use crate::database::{Database, ExecuteResult, SessionId, StatementResult, DEFAULT_DATABASE_NAME};

/// NexusDB gRPC service implementation.
pub struct NexusDbService {
    /// The database instance.
    db: Arc<Database>,
    /// Server start time.
    start_time: Instant,
    /// Maps transaction IDs to session IDs for persistent transactions.
    /// This allows transactions to span multiple gRPC calls.
    transaction_sessions: RwLock<HashMap<u64, SessionId>>,
    /// Next transaction ID counter.
    next_txn_id: AtomicU64,
}

impl NexusDbService {
    /// Creates a new gRPC service with the given database.
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            start_time: Instant::now(),
            transaction_sessions: RwLock::new(HashMap::new()),
            next_txn_id: AtomicU64::new(1),
        }
    }

    /// Extracts credentials from gRPC request metadata and authenticates.
    ///
    /// Supported metadata keys:
    /// - `authorization`: "Bearer <token>" (JWT) or "ApiKey <key>"
    /// - `x-nexus-username` + `x-nexus-password`: password auth
    ///
    /// Returns the verified identity or a gRPC `Status::unauthenticated` error.
    ///
    /// On successful authentication the event is recorded in the audit log.
    /// On failure the event is also recorded before returning the error.
    fn authenticate<T>(&self, request: &Request<T>) -> Result<Identity, Status> {
        let metadata = request.metadata();

        // Try to extract the client IP from the gRPC connection info
        let client_ip = request
            .remote_addr()
            .map(|addr| addr.to_string());

        // Try Authorization header first
        if let Some(auth_header) = metadata.get("authorization") {
            let auth_str = auth_header
                .to_str()
                .map_err(|_| Status::unauthenticated("invalid authorization header encoding"))?;

            let credential = if let Some(token) = auth_str.strip_prefix("Bearer ") {
                Credential::Jwt(token.to_string())
            } else if let Some(key) = auth_str.strip_prefix("ApiKey ") {
                Credential::ApiKey(key.to_string())
            } else {
                return Err(Status::unauthenticated(
                    "unsupported authorization scheme; use 'Bearer <jwt>' or 'ApiKey <key>'",
                ));
            };

            return self
                .db
                .authenticator()
                .authenticate(&credential)
                .map(|identity| {
                    self.db
                        .audit_log()
                        .record_auth(&identity.username, true, client_ip.clone());
                    identity
                })
                .map_err(|e| {
                    self.db
                        .audit_log()
                        .record_auth("unknown", false, client_ip);
                    Status::unauthenticated(format!("authentication failed: {}", e))
                });
        }

        // Try username + password from metadata
        if let (Some(username), Some(password)) = (
            metadata.get("x-nexus-username"),
            metadata.get("x-nexus-password"),
        ) {
            let username = username
                .to_str()
                .map_err(|_| Status::unauthenticated("invalid username encoding"))?
                .to_string();
            let password = password
                .to_str()
                .map_err(|_| Status::unauthenticated("invalid password encoding"))?
                .to_string();

            let credential = Credential::Password {
                username: username.clone(),
                password,
            };
            return self
                .db
                .authenticator()
                .authenticate(&credential)
                .map(|identity| {
                    self.db
                        .audit_log()
                        .record_auth(&identity.username, true, client_ip.clone());
                    identity
                })
                .map_err(|e| {
                    self.db
                        .audit_log()
                        .record_auth(&username, false, client_ip);
                    Status::unauthenticated(format!("authentication failed: {}", e))
                });
        }

        // No credentials provided
        if self.db.authenticator().is_enforcing() {
            Err(Status::unauthenticated(
                "authentication required; provide 'authorization' header or 'x-nexus-username'/'x-nexus-password' metadata",
            ))
        } else {
            // Permissive mode: return a default superuser identity
            Ok(Identity::system())
        }
    }

    /// Maps a [`DatabaseError`] to the correct gRPC proto [`ErrorCode`].
    fn map_error_code(err: &crate::database::DatabaseError) -> ErrorCode {
        match err {
            crate::database::DatabaseError::ParseError(_) => ErrorCode::SyntaxError,
            crate::database::DatabaseError::PlanError(_) => ErrorCode::InvalidQuery,
            crate::database::DatabaseError::ExecutionError(_) => ErrorCode::InternalError,
            crate::database::DatabaseError::TransactionError(_) => ErrorCode::TransactionNotFound,
            crate::database::DatabaseError::AuthenticationError(_) => {
                ErrorCode::AuthenticationFailed
            }
            crate::database::DatabaseError::AuthorizationError(_) => {
                ErrorCode::AuthorizationFailed
            }
            crate::database::DatabaseError::StorageError(_) => ErrorCode::InternalError,
            _ => ErrorCode::InternalError,
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

        let proto_rows: Vec<Row> = result
            .rows()
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
        // Authenticate the request
        let identity = self.authenticate(&request)?;

        let req = request.into_inner();
        let start = Instant::now();
        let database = req.database.as_deref().unwrap_or(DEFAULT_DATABASE_NAME);

        // Execute the SQL using authenticated session
        let result = if let Some(txn_id) = req.transaction_id {
            // Use the existing transaction session (identity was set at begin_transaction)
            let sessions = self.transaction_sessions.read().unwrap();
            if let Some(&session_id) = sessions.get(&txn_id) {
                if let Some(session_arc) = self.db.get_session(session_id) {
                    let mut session = session_arc.write().unwrap();
                    session.execute(&req.sql)
                } else {
                    Err(crate::database::DatabaseError::TransactionError(format!(
                        "session for transaction {} not found",
                        txn_id
                    )))
                }
            } else {
                Err(crate::database::DatabaseError::TransactionError(format!(
                    "transaction {} not found",
                    txn_id
                )))
            }
        } else {
            // Create a temporary authenticated session (autocommit)
            let session_id = self.db.create_authenticated_session(database, identity);
            let result = {
                let session_arc = self.db.get_session(session_id).unwrap();
                let mut session = session_arc.write().unwrap();
                session.execute(&req.sql)
            };
            self.db.close_session(session_id);
            result
        };

        match result {
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
                let error_code = Self::map_error_code(&e);
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
        // Authenticate the request
        let identity = self.authenticate(&request)?;

        let req = request.into_inner();
        let database = req.database.as_deref().unwrap_or(DEFAULT_DATABASE_NAME);

        // Execute the query on the requested database with authenticated identity
        let session_id = self.db.create_authenticated_session(database, identity);
        let result = {
            let session_arc = self.db.get_session(session_id).unwrap();
            let mut session = session_arc.write().unwrap();
            session.execute(&req.sql)
        };
        self.db.close_session(session_id);

        let result = result.map_err(|e| Status::internal(format!("Query execution failed: {}", e)))?;

        // Convert to a stream
        let rows: Vec<Row> = match result {
            StatementResult::Query(exec_result) => exec_result
                .rows()
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
                .collect(),
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
        request: Request<PrepareRequest>,
    ) -> Result<Response<PrepareResponse>, Status> {
        // Authenticate the request (even for unimplemented endpoints)
        let _identity = self.authenticate(&request)?;

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
        request: Request<ExecutePreparedRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        // Authenticate the request
        let _identity = self.authenticate(&request)?;

        Ok(Response::new(self.error_response(
            "Prepared statements not yet implemented".to_string(),
            ErrorCode::InternalError,
        )))
    }

    /// Begin a new transaction.
    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        // Authenticate the request
        let identity = self.authenticate(&request)?;

        // Create a persistent authenticated session for this transaction
        let session_id = self
            .db
            .create_authenticated_session(DEFAULT_DATABASE_NAME, identity);

        // Begin the transaction in this session
        if let Some(session_arc) = self.db.get_session(session_id) {
            let mut session = session_arc.write().unwrap();
            match session.begin() {
                Ok(()) => {
                    // Generate a unique transaction ID and map it to this session
                    let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);

                    let mut sessions = self.transaction_sessions.write().unwrap();
                    sessions.insert(txn_id, session_id);

                    Ok(Response::new(BeginTransactionResponse {
                        success: true,
                        error: None,
                        transaction_id: txn_id,
                    }))
                }
                Err(e) => {
                    // Clean up the session on error
                    self.db.close_session(session_id);
                    Ok(Response::new(BeginTransactionResponse {
                        success: false,
                        error: Some(e.to_string()),
                        transaction_id: 0,
                    }))
                }
            }
        } else {
            Ok(Response::new(BeginTransactionResponse {
                success: false,
                error: Some("Failed to create session".to_string()),
                transaction_id: 0,
            }))
        }
    }

    /// Commit a transaction.
    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        // Authenticate the request before accessing the transaction
        let _identity = self.authenticate(&request)?;

        let req = request.into_inner();
        let txn_id = req.transaction_id;

        // Find the session for this transaction
        let session_id = {
            let sessions = self.transaction_sessions.read().unwrap();
            sessions.get(&txn_id).copied()
        };

        if let Some(session_id) = session_id {
            if let Some(session_arc) = self.db.get_session(session_id) {
                let mut session = session_arc.write().unwrap();
                match session.commit() {
                    Ok(()) => {
                        drop(session);
                        // Clean up: remove the transaction mapping and close the session
                        {
                            let mut sessions = self.transaction_sessions.write().unwrap();
                            sessions.remove(&txn_id);
                        }
                        self.db.close_session(session_id);

                        Ok(Response::new(CommitResponse {
                            success: true,
                            error: None,
                        }))
                    }
                    Err(e) => {
                        // On commit failure, the session/transaction is no longer usable.
                        // Clean it up to avoid leaking sessions.
                        drop(session);
                        {
                            let mut sessions = self.transaction_sessions.write().unwrap();
                            sessions.remove(&txn_id);
                        }
                        self.db.close_session(session_id);

                        Ok(Response::new(CommitResponse {
                            success: false,
                            error: Some(e.to_string()),
                        }))
                    }
                }
            } else {
                Ok(Response::new(CommitResponse {
                    success: false,
                    error: Some("Session not found".to_string()),
                }))
            }
        } else {
            Ok(Response::new(CommitResponse {
                success: false,
                error: Some(format!("Transaction {} not found", txn_id)),
            }))
        }
    }

    /// Rollback a transaction.
    async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        // Authenticate the request before accessing the transaction
        let _identity = self.authenticate(&request)?;

        let req = request.into_inner();
        let txn_id = req.transaction_id;

        // Find the session for this transaction
        let session_id = {
            let sessions = self.transaction_sessions.read().unwrap();
            sessions.get(&txn_id).copied()
        };

        if let Some(session_id) = session_id {
            if let Some(session_arc) = self.db.get_session(session_id) {
                let mut session = session_arc.write().unwrap();
                match session.rollback() {
                    Ok(()) => {
                        drop(session);
                        // Clean up: remove the transaction mapping and close the session
                        {
                            let mut sessions = self.transaction_sessions.write().unwrap();
                            sessions.remove(&txn_id);
                        }
                        self.db.close_session(session_id);

                        Ok(Response::new(RollbackResponse {
                            success: true,
                            error: None,
                        }))
                    }
                    Err(e) => {
                        // On rollback failure, also tear down the session to avoid leaks.
                        drop(session);
                        {
                            let mut sessions = self.transaction_sessions.write().unwrap();
                            sessions.remove(&txn_id);
                        }
                        self.db.close_session(session_id);

                        Ok(Response::new(RollbackResponse {
                            success: false,
                            error: Some(e.to_string()),
                        }))
                    }
                }
            } else {
                Ok(Response::new(RollbackResponse {
                    success: false,
                    error: Some("Session not found".to_string()),
                }))
            }
        } else {
            Ok(Response::new(RollbackResponse {
                success: false,
                error: Some(format!("Transaction {} not found", txn_id)),
            }))
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
                "TLS".to_string(),
                "mTLS".to_string(),
                "Authentication".to_string(),
                "RBAC".to_string(),
                "AuditLog".to_string(),
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
    /// TLS configuration (None = plaintext).
    tls_config: Option<TlsConfig>,
}

/// TLS configuration for the gRPC server.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Server certificate (PEM).
    pub server_cert: Vec<u8>,
    /// Server private key (PEM).
    pub server_key: Vec<u8>,
    /// CA certificate for client verification (PEM). If set, enables mTLS.
    pub client_ca_cert: Option<Vec<u8>>,
}

impl GrpcServer {
    /// Creates a new gRPC server builder.
    pub fn new(db: Arc<Database>, addr: std::net::SocketAddr) -> Self {
        Self {
            db,
            addr,
            tls_config: None,
        }
    }

    /// Configures TLS from PEM-encoded certificate and key bytes.
    pub fn with_tls(mut self, cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        self.tls_config = Some(TlsConfig {
            server_cert: cert_pem,
            server_key: key_pem,
            client_ca_cert: None,
        });
        self
    }

    /// Configures mutual TLS (mTLS) with a CA certificate for client verification.
    pub fn with_mtls(
        mut self,
        cert_pem: Vec<u8>,
        key_pem: Vec<u8>,
        ca_cert_pem: Vec<u8>,
    ) -> Self {
        self.tls_config = Some(TlsConfig {
            server_cert: cert_pem,
            server_key: key_pem,
            client_ca_cert: Some(ca_cert_pem),
        });
        self
    }

    /// Configures TLS from file paths. Returns an error if the files cannot be read.
    pub fn with_tls_files(
        self,
        cert_path: &std::path::Path,
        key_path: &std::path::Path,
        ca_cert_path: Option<&std::path::Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cert_pem = std::fs::read(cert_path)?;
        let key_pem = std::fs::read(key_path)?;
        let ca_cert_pem = if let Some(ca_path) = ca_cert_path {
            Some(std::fs::read(ca_path)?)
        } else {
            None
        };

        Ok(if let Some(ca) = ca_cert_pem {
            self.with_mtls(cert_pem, key_pem, ca)
        } else {
            self.with_tls(cert_pem, key_pem)
        })
    }

    /// Starts the gRPC server.
    ///
    /// If TLS is configured, the server will use TLS 1.2+ (tonic/rustls default).
    /// If mTLS is configured (CA cert provided), clients must present a valid
    /// certificate signed by the CA.
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let service = NexusDbService::new(self.db);

        match self.tls_config {
            Some(tls) => {
                use tonic::transport::{Certificate, Identity, ServerTlsConfig};

                let identity = Identity::from_pem(&tls.server_cert, &tls.server_key);
                let mut tls_config = ServerTlsConfig::new().identity(identity);

                if let Some(ca_cert) = &tls.client_ca_cert {
                    let ca = Certificate::from_pem(ca_cert);
                    tls_config = tls_config.client_ca_root(ca);
                    tracing::info!(
                        "NexusDB gRPC server listening on {} (mTLS enabled)",
                        self.addr
                    );
                } else {
                    tracing::info!(
                        "NexusDB gRPC server listening on {} (TLS enabled)",
                        self.addr
                    );
                }

                tonic::transport::Server::builder()
                    .tls_config(tls_config)?
                    .add_service(nexus_proto::NexusDbServer::new(service))
                    .serve(self.addr)
                    .await?;
            }
            None => {
                tracing::info!(
                    "NexusDB gRPC server listening on {} (plaintext)",
                    self.addr
                );

                tonic::transport::Server::builder()
                    .add_service(nexus_proto::NexusDbServer::new(service))
                    .serve(self.addr)
                    .await?;
            }
        }

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
