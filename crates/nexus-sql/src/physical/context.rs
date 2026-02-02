//! Execution context and runtime configuration.
//!
//! The execution context provides runtime configuration and resources
//! for query execution, including memory limits, parallelism settings,
//! and catalog access.

use std::collections::HashMap;
use std::sync::Arc;

use crate::logical::{Schema, TableMeta};

/// Configuration for query execution.
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Maximum memory for execution in bytes.
    pub max_memory: usize,
    /// Target batch size for vectorized execution.
    pub batch_size: usize,
    /// Number of partitions for parallel execution.
    pub partitions: usize,
    /// Whether to collect execution metrics.
    pub collect_metrics: bool,
    /// Spill to disk threshold (fraction of max_memory).
    pub spill_threshold: f64,
    /// Enable parallel aggregation.
    pub parallel_aggregation: bool,
    /// Enable parallel join.
    pub parallel_join: bool,
    /// Hash join build side threshold in rows.
    pub hash_join_threshold: usize,
    /// Sort merge join preference threshold.
    pub merge_join_threshold: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_memory: 256 * 1024 * 1024, // 256 MB
            batch_size: 8192,
            partitions: num_cpus(),
            collect_metrics: false,
            spill_threshold: 0.8,
            parallel_aggregation: true,
            parallel_join: true,
            hash_join_threshold: 10_000,
            merge_join_threshold: 100_000,
        }
    }
}

impl ExecutionConfig {
    /// Creates a config optimized for small data.
    pub fn for_small_data() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024,
            batch_size: 1024,
            partitions: 1,
            ..Default::default()
        }
    }

    /// Creates a config optimized for large data.
    pub fn for_large_data() -> Self {
        Self {
            max_memory: 1024 * 1024 * 1024, // 1 GB
            batch_size: 16384,
            partitions: num_cpus() * 2,
            ..Default::default()
        }
    }
}

/// Returns the number of CPUs (defaults to 4 if detection fails).
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

/// Table information for execution.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name.
    pub name: String,
    /// Table schema.
    pub schema: Arc<Schema>,
    /// Table metadata.
    pub meta: Option<TableMeta>,
    /// Estimated row count.
    pub row_count: Option<usize>,
}

impl TableInfo {
    /// Creates new table info.
    pub fn new(name: impl Into<String>, schema: Schema) -> Self {
        Self {
            name: name.into(),
            schema: Arc::new(schema),
            meta: None,
            row_count: None,
        }
    }

    /// Sets the row count estimate.
    pub fn with_row_count(mut self, count: usize) -> Self {
        self.row_count = Some(count);
        self
    }

    /// Sets the table metadata.
    pub fn with_meta(mut self, meta: TableMeta) -> Self {
        self.meta = Some(meta);
        self
    }
}

/// Catalog interface for table lookups.
pub trait Catalog: std::fmt::Debug + Send + Sync {
    /// Looks up a table by name.
    fn get_table(&self, name: &str) -> Option<TableInfo>;

    /// Lists all table names.
    fn table_names(&self) -> Vec<String>;
}

/// Simple in-memory catalog for testing.
#[derive(Debug, Default)]
pub struct MemoryCatalog {
    tables: HashMap<String, TableInfo>,
}

impl MemoryCatalog {
    /// Creates a new empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a table.
    pub fn register_table(&mut self, info: TableInfo) {
        self.tables.insert(info.name.clone(), info);
    }

    /// Creates a catalog from a list of tables.
    pub fn with_tables(tables: Vec<TableInfo>) -> Self {
        let mut catalog = Self::new();
        for table in tables {
            catalog.register_table(table);
        }
        catalog
    }
}

impl Catalog for MemoryCatalog {
    fn get_table(&self, name: &str) -> Option<TableInfo> {
        self.tables.get(name).cloned()
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
}

/// Execution context for query processing.
///
/// Provides runtime configuration, catalog access, and resource management.
#[derive(Debug)]
pub struct ExecutionContext {
    /// Execution configuration.
    pub config: ExecutionConfig,
    /// Table catalog.
    catalog: Arc<dyn Catalog>,
    /// Session variables.
    variables: HashMap<String, String>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            config: ExecutionConfig::default(),
            catalog: Arc::new(MemoryCatalog::new()),
            variables: HashMap::new(),
        }
    }
}

impl ExecutionContext {
    /// Creates a new execution context with the given config.
    pub fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            catalog: Arc::new(MemoryCatalog::new()),
            variables: HashMap::new(),
        }
    }

    /// Creates a context with a catalog.
    pub fn with_catalog(config: ExecutionConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            config,
            catalog,
            variables: HashMap::new(),
        }
    }

    /// Returns the catalog.
    pub fn catalog(&self) -> &dyn Catalog {
        self.catalog.as_ref()
    }

    /// Looks up a table.
    pub fn get_table(&self, name: &str) -> Option<TableInfo> {
        self.catalog.get_table(name)
    }

    /// Sets a session variable.
    pub fn set_variable(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.variables.insert(name.into(), value.into());
    }

    /// Gets a session variable.
    pub fn get_variable(&self, name: &str) -> Option<&str> {
        self.variables.get(name).map(|s| s.as_str())
    }

    /// Returns the batch size.
    pub fn batch_size(&self) -> usize {
        self.config.batch_size
    }

    /// Returns the number of partitions.
    pub fn partitions(&self) -> usize {
        self.config.partitions
    }

    /// Returns maximum memory.
    pub fn max_memory(&self) -> usize {
        self.config.max_memory
    }
}

/// Execution metrics collected during query processing.
#[derive(Debug, Clone, Default)]
pub struct ExecutionMetrics {
    /// Number of rows processed.
    pub rows_processed: usize,
    /// Number of batches processed.
    pub batches_processed: usize,
    /// Execution time in microseconds.
    pub execution_time_us: u64,
    /// Peak memory usage in bytes.
    pub peak_memory: usize,
    /// Number of spills to disk.
    pub spill_count: usize,
    /// Bytes spilled to disk.
    pub bytes_spilled: usize,
    /// Per-operator metrics.
    pub operator_metrics: HashMap<String, OperatorMetrics>,
}

/// Metrics for a single operator.
#[derive(Debug, Clone, Default)]
pub struct OperatorMetrics {
    /// Operator name.
    pub name: String,
    /// Rows input.
    pub rows_in: usize,
    /// Rows output.
    pub rows_out: usize,
    /// Time spent in microseconds.
    pub time_us: u64,
    /// Memory used in bytes.
    pub memory: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::Field;
    use crate::parser::DataType;

    #[test]
    fn test_execution_config_default() {
        let config = ExecutionConfig::default();
        assert!(config.max_memory > 0);
        assert!(config.batch_size > 0);
        assert!(config.partitions > 0);
    }

    #[test]
    fn test_execution_context() {
        let ctx = ExecutionContext::default();
        assert!(ctx.batch_size() > 0);
        assert!(ctx.partitions() > 0);
    }

    #[test]
    fn test_memory_catalog() {
        let schema = Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
        ]);
        let table = TableInfo::new("users", schema);

        let mut catalog = MemoryCatalog::new();
        catalog.register_table(table);

        assert!(catalog.get_table("users").is_some());
        assert!(catalog.get_table("missing").is_none());
        assert_eq!(catalog.table_names().len(), 1);
    }

    #[test]
    fn test_session_variables() {
        let mut ctx = ExecutionContext::default();
        ctx.set_variable("timezone", "UTC");
        assert_eq!(ctx.get_variable("timezone"), Some("UTC"));
        assert_eq!(ctx.get_variable("missing"), None);
    }
}
