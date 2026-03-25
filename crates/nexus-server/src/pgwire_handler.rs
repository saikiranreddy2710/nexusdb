//! PostgreSQL wire protocol handler for NexusDB.
//!
//! Implements `SimpleQueryHandler` from the `pgwire` crate, enabling
//! standard PostgreSQL clients (`psql`, drivers, ORMs) to connect.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;
use futures::stream;
use tracing::{debug, error};

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::store::PortalStore;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireHandlerFactory, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use nexus_sql::executor::Value;
use nexus_sql::parser::DataType;

use crate::database::{Database, StatementResult};

// =========================================================================
// Type mapping
// =========================================================================

/// Maps NexusDB `DataType` to PostgreSQL `Type` OID.
fn datatype_to_pg(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::TinyInt => Type::INT2,
        DataType::SmallInt => Type::INT2,
        DataType::Int => Type::INT4,
        DataType::BigInt => Type::INT8,
        DataType::Float => Type::FLOAT4,
        DataType::Double => Type::FLOAT8,
        DataType::Decimal { .. } => Type::NUMERIC,
        DataType::Varchar(_) => Type::VARCHAR,
        DataType::Text => Type::TEXT,
        DataType::Blob => Type::BYTEA,
        DataType::Date => Type::DATE,
        DataType::Time => Type::TIME,
        DataType::Timestamp => Type::TIMESTAMP,
        _ => Type::TEXT,
    }
}

// =========================================================================
// Query Handler
// =========================================================================

/// NexusDB query handler for PostgreSQL wire protocol.
pub struct PgWireQueryHandler {
    db: Arc<Database>,
}

impl PgWireQueryHandler {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Executes one SQL statement and returns a pgwire Response.
    fn execute_one(&self, sql: &str) -> PgWireResult<Response<'_>> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Ok(Response::EmptyQuery);
        }

        // Create a temporary session, execute, then close
        let session_id = self.db.create_session("nexusdb");

        let result = {
            let session_arc = self.db.get_session(session_id).ok_or_else(|| {
                PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "failed to create session",
                )))
            })?;
            let mut session = session_arc.write().unwrap();
            session.execute(trimmed)
        };

        self.db.close_session(session_id);

        match result {
            Ok(stmt_result) => self.convert_result(&stmt_result),
            Err(e) => Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    e.to_string(),
                ),
            ))),
        }
    }

    /// Converts a NexusDB StatementResult to a pgwire Response.
    fn convert_result(&self, result: &StatementResult) -> PgWireResult<Response<'_>> {
        match result {
            StatementResult::Query(exec_result) => {
                // Build column metadata
                let fields: Vec<FieldInfo> = exec_result
                    .schema
                    .fields()
                    .iter()
                    .map(|f| {
                        let pg_type = datatype_to_pg(&f.data_type);
                        FieldInfo::new(
                            f.name().to_string(),
                            None,
                            None,
                            pg_type,
                            FieldFormat::Text,
                        )
                    })
                    .collect();
                let schema = Arc::new(fields);

                // Encode rows
                let rows = exec_result.rows();
                let mut encoded = Vec::with_capacity(rows.len());
                for row in &rows {
                    let mut enc = DataRowEncoder::new(schema.clone());
                    for val in row.iter() {
                        self.encode_value(&mut enc, val)?;
                    }
                    encoded.push(Ok(enc.finish()?));
                }

                Ok(Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(encoded),
                )))
            }
            StatementResult::Insert { rows_affected } => {
                Ok(Response::Execution(
                    Tag::new("INSERT").with_rows(*rows_affected as usize),
                ))
            }
            StatementResult::Update { rows_affected } => {
                Ok(Response::Execution(
                    Tag::new("UPDATE").with_rows(*rows_affected as usize),
                ))
            }
            StatementResult::Delete { rows_affected } => {
                Ok(Response::Execution(
                    Tag::new("DELETE").with_rows(*rows_affected as usize),
                ))
            }
            StatementResult::Ddl { command } => {
                Ok(Response::Execution(Tag::new(command)))
            }
            StatementResult::Transaction { command } => {
                Ok(Response::Execution(Tag::new(command)))
            }
            StatementResult::Empty => Ok(Response::EmptyQuery),
        }
    }

    /// Encodes a NexusDB Value into a pgwire column.
    fn encode_value(&self, enc: &mut DataRowEncoder, val: &Value) -> PgWireResult<()> {
        match val {
            Value::Null => enc.encode_field(&None::<&str>),
            Value::Boolean(b) => enc.encode_field(b),
            Value::TinyInt(i) => enc.encode_field(&(*i as i16)),
            Value::SmallInt(i) => enc.encode_field(i),
            Value::Int(i) => enc.encode_field(i),
            Value::BigInt(i) => enc.encode_field(i),
            Value::Float(f) => enc.encode_field(f),
            Value::Double(f) => enc.encode_field(f),
            Value::String(s) => enc.encode_field(&s.as_str()),
            Value::Bytes(b) => enc.encode_field(b),
            Value::Date(d) => enc.encode_field(&d.to_string().as_str()),
            Value::Time(t) => enc.encode_field(&t.to_string().as_str()),
            Value::Timestamp(ts) => enc.encode_field(&ts.to_string().as_str()),
            Value::Decimal { value, scale } => {
                let s = format_decimal(*value, *scale);
                enc.encode_field(&s.as_str())
            }
            Value::Vector(v) => {
                let s = format!("{:?}", v);
                enc.encode_field(&s.as_str())
            }
        }
    }
}

/// Formats a decimal value as a string.
fn format_decimal(value: i128, scale: u8) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let divisor = 10_i128.pow(scale as u32);
    let integer_part = value / divisor;
    let fractional_part = (value % divisor).abs();
    format!("{}.{:0>width$}", integer_part, fractional_part, width = scale as usize)
}

#[async_trait]
impl SimpleQueryHandler for PgWireQueryHandler {
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        debug!("pgwire query: {}", query);

        // Split multi-statement queries
        let statements: Vec<&str> = query
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        let mut responses = Vec::with_capacity(statements.len());
        for stmt in statements {
            match self.execute_one(stmt) {
                Ok(resp) => responses.push(resp),
                Err(e) => {
                    error!("pgwire error: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(responses)
    }
}

// =========================================================================
// Extended Query Handler (Parse/Bind/Describe/Execute/Sync)
// =========================================================================

#[async_trait]
impl ExtendedQueryHandler for PgWireQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::default())
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &portal.statement.statement;
        debug!("pgwire extended query: {}", sql);
        self.execute_one(sql)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // For DESCRIBE, return empty param types and inferred result columns.
        // A full implementation would parse the SQL to infer column types.
        Ok(DescribeStatementResponse::new(vec![], vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribePortalResponse::new(vec![]))
    }
}

// =========================================================================
// Handler Factory
// =========================================================================

/// Factory providing handler instances for each pgwire connection.
pub struct NexusPgWireFactory {
    handler: Arc<PgWireQueryHandler>,
}

impl NexusPgWireFactory {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            handler: Arc::new(PgWireQueryHandler::new(db)),
        }
    }
}

impl PgWireHandlerFactory for NexusPgWireFactory {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = PgWireQueryHandler;
    type ExtendedQueryHandler = PgWireQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}
