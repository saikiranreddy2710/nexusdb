//! Client connection management.
//!
//! Provides the main `Client` struct for connecting to NexusDB and executing queries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::Mutex as AsyncMutex;
use tonic::transport::Channel;

use nexus_proto::proto::{
    nexus_db_client::NexusDbClient, BeginTransactionRequest, CommitRequest, ExecuteRequest,
    PingRequest, RollbackRequest, ServerInfoRequest,
};

use super::error::{ClientError, ClientResult, ConnectionState};
use super::query::QueryBuilder;
use super::transaction::Transaction;

/// Client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Server host.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Query timeout.
    pub query_timeout: Duration,
    /// Whether to use TLS.
    pub use_tls: bool,
    /// Database name.
    pub database: Option<String>,
    /// Username for authentication.
    pub username: Option<String>,
    /// Password for authentication.
    pub password: Option<String>,
    /// Application name for identification.
    pub application_name: Option<String>,
    /// Auto-reconnect on failure.
    pub auto_reconnect: bool,
    /// Maximum reconnect attempts.
    pub max_reconnect_attempts: u32,
    /// Reconnect delay.
    pub reconnect_delay: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            connect_timeout: Duration::from_secs(10),
            query_timeout: Duration::from_secs(300),
            use_tls: false,
            database: None,
            username: None,
            password: None,
            application_name: Some("nexus-client".to_string()),
            auto_reconnect: true,
            max_reconnect_attempts: 3,
            reconnect_delay: Duration::from_millis(500),
        }
    }
}

impl ClientConfig {
    /// Creates a new client configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the host.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Sets the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the query timeout.
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// Enables TLS.
    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    /// Sets the database name.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Sets the username.
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Sets the password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Sets the application name.
    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.application_name = Some(name.into());
        self
    }

    /// Enables auto-reconnect.
    pub fn auto_reconnect(mut self, auto_reconnect: bool) -> Self {
        self.auto_reconnect = auto_reconnect;
        self
    }

    /// Returns the connection string.
    pub fn connection_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Query result from the server.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names.
    pub columns: Vec<String>,
    /// Row data.
    pub rows: Vec<Vec<Value>>,
    /// Number of rows affected (for DML).
    pub rows_affected: u64,
    /// Execution time.
    pub execution_time: Duration,
}

impl QueryResult {
    /// Creates an empty result.
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
            execution_time: Duration::ZERO,
        }
    }

    /// Creates a result with affected rows count.
    pub fn affected(count: u64, execution_time: Duration) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: count,
            execution_time,
        }
    }

    /// Returns true if the result has rows.
    pub fn has_rows(&self) -> bool {
        !self.rows.is_empty()
    }

    /// Returns the number of rows.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Gets a single value from the first row.
    pub fn get_value(&self, column: usize) -> Option<&Value> {
        self.rows.first().and_then(|row| row.get(column))
    }

    /// Iterates over rows.
    pub fn iter(&self) -> impl Iterator<Item = &Vec<Value>> {
        self.rows.iter()
    }
}

/// Value types supported by the client.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Null value.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// Integer value.
    Integer(i64),
    /// Float value.
    Float(f64),
    /// String value.
    String(String),
    /// Binary data.
    Bytes(Vec<u8>),
}

impl Value {
    /// Returns true if the value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Tries to get as boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Tries to get as integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Tries to get as float.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Tries to get as string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Tries to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "{}", s),
            Value::Bytes(b) => write!(f, "<{} bytes>", b.len()),
        }
    }
}

/// Statistics about client usage.
#[derive(Debug, Clone, Default)]
pub struct ClientStats {
    /// Total queries executed.
    pub queries_executed: u64,
    /// Total query time.
    pub total_query_time_ms: u64,
    /// Number of transactions.
    pub transactions: u64,
    /// Number of committed transactions.
    pub commits: u64,
    /// Number of rolled back transactions.
    pub rollbacks: u64,
    /// Connection attempts.
    pub connection_attempts: u64,
    /// Successful connections.
    pub successful_connections: u64,
    /// Connection failures.
    pub connection_failures: u64,
}

/// Server information.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    /// Server version.
    pub version: String,
    /// Server name.
    pub server_name: String,
    /// Protocol version.
    pub protocol_version: u32,
    /// Supported features.
    pub features: Vec<String>,
    /// Server uptime in seconds.
    pub uptime_seconds: u64,
    /// Number of active connections.
    pub active_connections: u32,
}

/// Internal connection state.
struct ConnectionInner {
    /// Connection state.
    state: ConnectionState,
    /// When the connection was established.
    connected_at: Option<Instant>,
    /// Last activity time.
    last_activity: Instant,
    /// Current transaction ID.
    transaction_id: Option<u64>,
    /// gRPC client (when connected).
    grpc_client: Option<NexusDbClient<Channel>>,
}

/// NexusDB client.
pub struct Client {
    /// Configuration.
    config: ClientConfig,
    /// Connection state.
    inner: RwLock<ConnectionInner>,
    /// Async lock for operations.
    op_lock: AsyncMutex<()>,
    /// Query counter.
    query_counter: AtomicU64,
    /// Transaction counter.
    txn_counter: AtomicU64,
    /// Statistics.
    stats: RwLock<ClientStats>,
}

impl Client {
    /// Creates a new client with the given configuration.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            inner: RwLock::new(ConnectionInner {
                state: ConnectionState::Disconnected,
                connected_at: None,
                last_activity: Instant::now(),
                transaction_id: None,
                grpc_client: None,
            }),
            op_lock: AsyncMutex::new(()),
            query_counter: AtomicU64::new(0),
            txn_counter: AtomicU64::new(0),
            stats: RwLock::new(ClientStats::default()),
        }
    }

    /// Creates a new client with default configuration.
    /// 
    /// This creates a client in "mock" mode for testing purposes.
    /// For real connections, use `Client::new(config).connect().await`.
    pub fn connect_default() -> ClientResult<Self> {
        let client = Self::new(ClientConfig::default());
        // Mark as connected in mock mode (no gRPC client)
        {
            let mut inner = client.inner.write();
            inner.state = ConnectionState::Connected;
            inner.connected_at = Some(Instant::now());
        }
        Ok(client)
    }

    /// Pings the server to check connectivity.
    pub async fn ping(&self) -> ClientResult<Duration> {
        self.ensure_connected()?;

        let mut client = {
            let inner = self.inner.read();
            match &inner.grpc_client {
                Some(c) => c.clone(),
                None => {
                    // Mock mode - return instant response
                    return Ok(Duration::from_micros(1));
                }
            }
        };

        let start = Instant::now();
        let request = tonic::Request::new(PingRequest {
            payload: Some(b"ping".to_vec()),
        });

        client
            .ping(request)
            .await
            .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?;

        Ok(start.elapsed())
    }

    /// Gets server information.
    pub async fn server_info(&self) -> ClientResult<ServerInfo> {
        self.ensure_connected()?;

        let mut client = {
            let inner = self.inner.read();
            match &inner.grpc_client {
                Some(c) => c.clone(),
                None => {
                    // Mock mode - return mock info
                    return Ok(ServerInfo {
                        version: "mock".to_string(),
                        server_name: "NexusDB (mock)".to_string(),
                        protocol_version: 1,
                        features: vec!["mock".to_string()],
                        uptime_seconds: 0,
                        active_connections: 1,
                    });
                }
            }
        };

        let request = tonic::Request::new(ServerInfoRequest {});

        let response = client
            .get_server_info(request)
            .await
            .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?
            .into_inner();

        Ok(ServerInfo {
            version: response.version,
            server_name: response.server_name,
            protocol_version: response.protocol_version,
            features: response.features,
            uptime_seconds: response.uptime_seconds,
            active_connections: response.active_connections,
        })
    }

    /// Connects to the server.
    pub async fn connect(&self) -> ClientResult<()> {
        let _lock = self.op_lock.lock().await;

        {
            let mut stats = self.stats.write();
            stats.connection_attempts += 1;
        }

        // Update state
        {
            let mut inner = self.inner.write();
            inner.state = ConnectionState::Connecting;
        }

        // Build the endpoint URL
        let scheme = if self.config.use_tls { "https" } else { "http" };
        let endpoint = format!("{}://{}:{}", scheme, self.config.host, self.config.port);

        // Connect to the gRPC server with timeout
        let connect_result = tokio::time::timeout(
            self.config.connect_timeout,
            NexusDbClient::connect(endpoint.clone()),
        )
        .await;

        match connect_result {
            Ok(Ok(client)) => {
                {
                    let mut inner = self.inner.write();
                    inner.state = ConnectionState::Connected;
                    inner.connected_at = Some(Instant::now());
                    inner.last_activity = Instant::now();
                    inner.grpc_client = Some(client);
                }

                {
                    let mut stats = self.stats.write();
                    stats.successful_connections += 1;
                }

                Ok(())
            }
            Ok(Err(e)) => {
                {
                    let mut inner = self.inner.write();
                    inner.state = ConnectionState::Failed;
                }

                {
                    let mut stats = self.stats.write();
                    stats.connection_failures += 1;
                }

                Err(ClientError::ConnectionFailed(format!(
                    "failed to connect to {}: {}",
                    endpoint, e
                )))
            }
            Err(_) => {
                {
                    let mut inner = self.inner.write();
                    inner.state = ConnectionState::Failed;
                }

                {
                    let mut stats = self.stats.write();
                    stats.connection_failures += 1;
                }

                Err(ClientError::Timeout)
            }
        }
    }

    /// Disconnects from the server.
    pub async fn disconnect(&self) -> ClientResult<()> {
        let _lock = self.op_lock.lock().await;

        let has_transaction = {
            let inner = self.inner.read();
            inner.transaction_id.is_some()
        };

        if has_transaction {
            // Rollback any active transaction
            self.rollback_internal().await?;
        }

        {
            let mut inner = self.inner.write();
            inner.state = ConnectionState::Closed;
            inner.connected_at = None;
            inner.transaction_id = None;
            inner.grpc_client = None;
        }

        Ok(())
    }

    /// Returns the connection state.
    pub fn state(&self) -> ConnectionState {
        self.inner.read().state
    }

    /// Returns true if connected.
    pub fn is_connected(&self) -> bool {
        let state = self.inner.read().state;
        matches!(
            state,
            ConnectionState::Connected | ConnectionState::InTransaction
        )
    }

    /// Returns true if in a transaction.
    pub fn in_transaction(&self) -> bool {
        self.inner.read().transaction_id.is_some()
    }

    /// Returns the configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Returns client statistics.
    pub fn stats(&self) -> ClientStats {
        self.stats.read().clone()
    }

    /// Returns how long the connection has been open.
    pub fn connection_duration(&self) -> Option<Duration> {
        self.inner.read().connected_at.map(|t| t.elapsed())
    }

    // =========================================================================
    // Query Execution
    // =========================================================================

    /// Executes a SQL query.
    pub async fn execute(&self, sql: &str) -> ClientResult<QueryResult> {
        self.ensure_connected()?;

        let start = Instant::now();
        let query_id = self.query_counter.fetch_add(1, Ordering::Relaxed);

        // Update last activity
        {
            let mut inner = self.inner.write();
            inner.last_activity = Instant::now();
        }

        // Execute query via gRPC (falls back to mock mode if not connected)
        let result = self.execute_query_internal(sql, query_id).await?;

        let elapsed = start.elapsed();

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.queries_executed += 1;
            stats.total_query_time_ms += elapsed.as_millis() as u64;
        }

        Ok(QueryResult {
            execution_time: elapsed,
            ..result
        })
    }

    /// Executes a query and returns a single value.
    pub async fn query_one<T>(&self, sql: &str) -> ClientResult<Option<T>>
    where
        T: FromValue,
    {
        let result = self.execute(sql).await?;
        if let Some(row) = result.rows.first() {
            if let Some(value) = row.first() {
                return Ok(T::from_value(value));
            }
        }
        Ok(None)
    }

    /// Executes a query and returns all rows as a vector.
    pub async fn query_all(&self, sql: &str) -> ClientResult<Vec<Vec<Value>>> {
        let result = self.execute(sql).await?;
        Ok(result.rows)
    }

    /// Creates a query builder.
    pub fn query(&self) -> QueryBuilder<'_> {
        QueryBuilder::new(self)
    }

    // =========================================================================
    // Transactions
    // =========================================================================

    /// Begins a new transaction.
    pub async fn begin(&self) -> ClientResult<Transaction<'_>> {
        self.ensure_connected()?;

        let _lock = self.op_lock.lock().await;

        // Check if already in transaction
        {
            let inner = self.inner.read();
            if inner.transaction_id.is_some() {
                return Err(ClientError::TransactionActive);
            }
        }

        // Get gRPC client (if available)
        let client_opt = {
            let inner = self.inner.read();
            inner.grpc_client.clone()
        };

        let txn_id = if let Some(mut client) = client_opt {
            // Execute BEGIN via gRPC
            let request = tonic::Request::new(BeginTransactionRequest {
                isolation_level: 0, // Default: READ_UNCOMMITTED
                read_only: false,
                timeout_ms: self.config.query_timeout.as_millis() as u32,
            });

            let response = client
                .begin_transaction(request)
                .await
                .map_err(|e| ClientError::TransactionError(e.to_string()))?
                .into_inner();

            if !response.success {
                return Err(ClientError::TransactionError(
                    response.error.unwrap_or_else(|| "failed to begin transaction".to_string()),
                ));
            }

            response.transaction_id
        } else {
            // Mock mode - generate a local transaction ID
            self.txn_counter.fetch_add(1, Ordering::Relaxed)
        };

        {
            let mut inner = self.inner.write();
            inner.state = ConnectionState::InTransaction;
            inner.transaction_id = Some(txn_id);
        }

        {
            let mut stats = self.stats.write();
            stats.transactions += 1;
        }

        Ok(Transaction::new(self, txn_id))
    }

    /// Commits the current transaction.
    pub async fn commit(&self) -> ClientResult<()> {
        let _lock = self.op_lock.lock().await;
        self.commit_internal().await
    }

    /// Internal commit without lock.
    async fn commit_internal(&self) -> ClientResult<()> {
        let txn_id = {
            let inner = self.inner.read();
            match inner.transaction_id {
                Some(id) => id,
                None => return Err(ClientError::NoTransaction),
            }
        };

        // Get gRPC client (if available)
        let client_opt = {
            let inner = self.inner.read();
            inner.grpc_client.clone()
        };

        if let Some(mut client) = client_opt {
            // Execute COMMIT via gRPC
            let request = tonic::Request::new(CommitRequest {
                transaction_id: txn_id,
            });

            let response = client
                .commit(request)
                .await
                .map_err(|e| ClientError::TransactionError(e.to_string()))?
                .into_inner();

            if !response.success {
                return Err(ClientError::TransactionError(
                    response.error.unwrap_or_else(|| "failed to commit".to_string()),
                ));
            }
        }
        // Mock mode: no gRPC call needed

        {
            let mut inner = self.inner.write();
            inner.state = ConnectionState::Connected;
            inner.transaction_id = None;
        }

        {
            let mut stats = self.stats.write();
            stats.commits += 1;
        }

        Ok(())
    }

    /// Rolls back the current transaction.
    pub async fn rollback(&self) -> ClientResult<()> {
        let _lock = self.op_lock.lock().await;
        self.rollback_internal().await
    }

    /// Internal rollback without lock.
    async fn rollback_internal(&self) -> ClientResult<()> {
        let txn_id = {
            let inner = self.inner.read();
            match inner.transaction_id {
                Some(id) => id,
                None => return Ok(()), // Rollback on no transaction is a no-op
            }
        };

        // Execute ROLLBACK via gRPC
        let client_opt = {
            let inner = self.inner.read();
            inner.grpc_client.clone()
        };

        if let Some(mut client) = client_opt {
            let request = tonic::Request::new(RollbackRequest {
                transaction_id: txn_id,
            });

            let _ = client.rollback(request).await; // Ignore errors on rollback
        }

        {
            let mut inner = self.inner.write();
            inner.state = ConnectionState::Connected;
            inner.transaction_id = None;
        }

        {
            let mut stats = self.stats.write();
            stats.rollbacks += 1;
        }

        Ok(())
    }

    /// Commits a specific transaction.
    pub(crate) async fn commit_transaction(&self, _txn_id: u64) -> ClientResult<()> {
        self.commit().await
    }

    /// Rolls back a specific transaction.
    pub(crate) async fn rollback_transaction(&self, _txn_id: u64) -> ClientResult<()> {
        self.rollback().await
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    /// Ensures the client is connected.
    fn ensure_connected(&self) -> ClientResult<()> {
        let state = self.inner.read().state;
        match state {
            ConnectionState::Connected | ConnectionState::InTransaction => Ok(()),
            ConnectionState::Disconnected | ConnectionState::Connecting => {
                Err(ClientError::ConnectionFailed("not connected".to_string()))
            }
            ConnectionState::Failed => {
                Err(ClientError::ConnectionFailed("connection failed".to_string()))
            }
            ConnectionState::Closed => Err(ClientError::ConnectionClosed),
        }
    }

    /// Internal query execution.
    async fn execute_query_internal(
        &self,
        sql: &str,
        _query_id: u64,
    ) -> ClientResult<QueryResult> {
        // Get a clone of the gRPC client
        let client_opt = {
            let inner = self.inner.read();
            inner.grpc_client.clone()
        };

        // If no gRPC client, we're in mock mode
        let mut client = match client_opt {
            Some(c) => c,
            None => {
                // Mock mode - return empty result
                return Ok(QueryResult::empty());
            }
        };

        // Execute the query via gRPC
        let request = tonic::Request::new(ExecuteRequest {
            sql: sql.to_string(),
            parameters: vec![],
            transaction_id: None,
            max_rows: 0, // Unlimited
            timeout_ms: self.config.query_timeout.as_millis() as u32,
        });

        let response = client
            .execute(request)
            .await
            .map_err(|e| ClientError::QueryFailed(e.to_string()))?
            .into_inner();

        if !response.success {
            return Err(ClientError::QueryFailed(
                response.error.unwrap_or_else(|| "unknown error".to_string()),
            ));
        }

        // Convert the response to QueryResult
        match response.result {
            Some(nexus_proto::proto::execute_response::Result::QueryResult(qr)) => {
                let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
                let rows: Vec<Vec<Value>> = qr
                    .rows
                    .iter()
                    .map(|row| {
                        row.values
                            .iter()
                            .map(|v| proto_value_to_value(v))
                            .collect()
                    })
                    .collect();

                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                    execution_time: Duration::ZERO,
                })
            }
            Some(nexus_proto::proto::execute_response::Result::CommandResult(cr)) => {
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: cr.rows_affected,
                    execution_time: Duration::ZERO,
                })
            }
            None => Ok(QueryResult::empty()),
        }
    }
}

/// Converts a proto Value to a client Value.
fn proto_value_to_value(v: &nexus_proto::proto::Value) -> Value {
    use nexus_proto::proto::value::Value as ProtoValue;

    match &v.value {
        Some(ProtoValue::IsNull(_)) => Value::Null,
        Some(ProtoValue::BoolValue(b)) => Value::Boolean(*b),
        Some(ProtoValue::Int32Value(i)) => Value::Integer(*i as i64),
        Some(ProtoValue::Int64Value(i)) => Value::Integer(*i),
        Some(ProtoValue::FloatValue(f)) => Value::Float(*f as f64),
        Some(ProtoValue::DoubleValue(f)) => Value::Float(*f),
        Some(ProtoValue::StringValue(s)) => Value::String(s.clone()),
        Some(ProtoValue::BytesValue(b)) => Value::Bytes(b.clone()),
        Some(ProtoValue::TimestampValue(t)) => Value::Integer(*t), // Store as epoch ms
        Some(ProtoValue::DateValue(d)) => Value::Integer(*d as i64), // Days since epoch
        Some(ProtoValue::TimeValue(t)) => Value::Integer(*t), // Microseconds
        Some(ProtoValue::DecimalValue(s)) => Value::String(s.clone()), // Keep as string
        None => Value::Null,
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("state", &self.state())
            .field("in_transaction", &self.in_transaction())
            .finish()
    }
}

/// Trait for converting from Value.
pub trait FromValue: Sized {
    /// Converts from a Value.
    fn from_value(value: &Value) -> Option<Self>;
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_i64()
    }
}

impl FromValue for i32 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_i64().map(|v| v as i32)
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_f64()
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_bool()
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_str().map(|s| s.to_string())
    }
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Option<Self> {
        Some(value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config() {
        let config = ClientConfig::new()
            .host("localhost")
            .port(5432)
            .database("test")
            .username("user")
            .password("pass");

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.database, Some("test".to_string()));
        assert_eq!(config.connection_string(), "localhost:5432");
    }

    #[test]
    fn test_value_conversions() {
        let int_val = Value::Integer(42);
        assert_eq!(int_val.as_i64(), Some(42));
        assert_eq!(int_val.as_f64(), Some(42.0));
        assert!(int_val.as_str().is_none());

        let str_val = Value::String("hello".to_string());
        assert_eq!(str_val.as_str(), Some("hello"));
        assert!(str_val.as_i64().is_none());

        let null_val = Value::Null;
        assert!(null_val.is_null());
    }

    #[test]
    fn test_query_result() {
        let result = QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec![Value::Integer(1), Value::String("Alice".to_string())],
                vec![Value::Integer(2), Value::String("Bob".to_string())],
            ],
            rows_affected: 0,
            execution_time: Duration::from_millis(10),
        };

        assert!(result.has_rows());
        assert_eq!(result.row_count(), 2);
        assert_eq!(result.column_count(), 2);
        assert_eq!(result.get_value(0), Some(&Value::Integer(1)));
    }

    #[tokio::test]
    async fn test_client_connect() {
        // Use mock mode for tests (no actual server connection)
        let client = Client::connect_default().unwrap();
        assert!(client.is_connected());
        assert_eq!(client.state(), ConnectionState::Connected);
    }

    #[tokio::test]
    async fn test_client_disconnect() {
        let client = Client::connect_default().unwrap();
        client.disconnect().await.unwrap();
        assert_eq!(client.state(), ConnectionState::Closed);
    }

    #[tokio::test]
    async fn test_client_stats() {
        let client = Client::connect_default().unwrap();

        client.execute("SELECT 1").await.unwrap();
        client.execute("SELECT 2").await.unwrap();

        let stats = client.stats();
        assert_eq!(stats.queries_executed, 2);
    }
}
