//! Logical plan builder.
//!
//! Converts parsed SQL statements into logical query plans.

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

use crate::parser::{
    self, DataType, DeleteStatement, Expr as AstExpr, FromItem, InsertSource, InsertStatement,
    OrderByExpr as AstOrderBy, SelectStatement, Statement, TableRef, UpdateStatement,
};

use super::expr::{AggregateFunc, BinaryOp, LogicalExpr, SortExpr, UnaryOp};
use super::operator::*;
use super::plan::LogicalPlan;
use super::schema::{Column, Field, Schema, SchemaRef, TableMeta};

/// Errors that can occur during plan building.
#[derive(Debug, Error)]
pub enum PlanError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Ambiguous column: {0}")]
    AmbiguousColumn(String),

    #[error("Type error: {0}")]
    TypeError(String),

    #[error("Unsupported: {0}")]
    Unsupported(String),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),
}

/// Result type for plan building.
pub type PlanResult<T> = Result<T, PlanError>;

/// Catalog interface for resolving table schemas.
pub trait Catalog: Send + Sync {
    /// Resolves a table by name.
    fn resolve_table(&self, name: &str) -> Option<TableMeta>;
}

/// Default catalog that returns empty schemas.
pub struct EmptyCatalog;

impl Catalog for EmptyCatalog {
    fn resolve_table(&self, _name: &str) -> Option<TableMeta> {
        None
    }
}

/// In-memory catalog for testing.
#[derive(Default)]
pub struct MemoryCatalog {
    tables: HashMap<String, TableMeta>,
}

impl MemoryCatalog {
    /// Creates an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a table to the catalog.
    pub fn add_table(&mut self, meta: TableMeta) {
        self.tables.insert(meta.name.clone(), meta);
    }
}

impl Catalog for MemoryCatalog {
    fn resolve_table(&self, name: &str) -> Option<TableMeta> {
        self.tables.get(name).cloned()
    }
}

/// Context for building logical plans.
pub struct PlanContext<'a> {
    /// Catalog for resolving tables.
    catalog: &'a dyn Catalog,
    /// Current scope schemas (for subqueries).
    scopes: Vec<SchemaRef>,
    /// CTE definitions.
    ctes: HashMap<String, Arc<LogicalOperator>>,
}

impl<'a> PlanContext<'a> {
    /// Creates a new plan context.
    pub fn new(catalog: &'a dyn Catalog) -> Self {
        Self {
            catalog,
            scopes: Vec::new(),
            ctes: HashMap::new(),
        }
    }

    /// Pushes a new scope.
    pub fn push_scope(&mut self, schema: SchemaRef) {
        self.scopes.push(schema);
    }

    /// Pops the current scope.
    pub fn pop_scope(&mut self) {
        self.scopes.pop();
    }

    /// Registers a CTE.
    pub fn register_cte(&mut self, name: String, plan: Arc<LogicalOperator>) {
        self.ctes.insert(name, plan);
    }

    /// Resolves a table reference.
    fn resolve_table(&self, table_ref: &TableRef) -> PlanResult<ScanOperator> {
        // First check CTEs
        if let Some(cte_plan) = self.ctes.get(&table_ref.table) {
            // For CTEs, we create a special scan that references the CTE
            let schema = cte_plan.schema();
            return Ok(ScanOperator {
                table_name: table_ref.table.clone(),
                table_schema: schema.clone(),
                projection: None,
                projected_schema: schema,
                filters: Vec::new(),
                limit: None,
                table_meta: None,
            });
        }

        // Then check catalog
        let full_name = if let Some(ref s) = table_ref.schema {
            format!("{}.{}", s, table_ref.table)
        } else {
            table_ref.table.clone()
        };

        match self.catalog.resolve_table(&full_name) {
            Some(meta) => Ok(ScanOperator {
                table_name: full_name,
                table_schema: meta.schema.clone(),
                projection: None,
                projected_schema: meta.schema.clone(),
                filters: Vec::new(),
                limit: None,
                table_meta: Some(meta),
            }),
            None => {
                // For flexibility, create an empty scan
                // Real implementation would error
                Ok(ScanOperator::new(&full_name, Schema::empty()))
            }
        }
    }
}

/// Builds a logical plan from a statement.
pub fn build_plan(stmt: &Statement, catalog: &dyn Catalog) -> PlanResult<LogicalPlan> {
    let mut ctx = PlanContext::new(catalog);
    let op = build_statement(stmt, &mut ctx)?;
    Ok(LogicalPlan::from_arc(op))
}

fn build_statement(stmt: &Statement, ctx: &mut PlanContext) -> PlanResult<Arc<LogicalOperator>> {
    match stmt {
        Statement::Select(select) => build_select(select, ctx),
        Statement::Insert(insert) => build_insert(insert, ctx),
        Statement::Update(update) => build_update(update, ctx),
        Statement::Delete(delete) => build_delete(delete, ctx),
        Statement::Explain(inner) => build_statement(inner, ctx),
        Statement::ExplainAnalyze(inner) => build_statement(inner, ctx),
        _ => Err(PlanError::Unsupported(format!(
            "Statement type: {:?}",
            std::mem::discriminant(stmt)
        ))),
    }
}

fn build_select(
    select: &SelectStatement,
    ctx: &mut PlanContext,
) -> PlanResult<Arc<LogicalOperator>> {
    // Handle CTEs first
    for cte in &select.ctes {
        let cte_plan = build_select(&cte.query, ctx)?;
        ctx.register_cte(cte.name.clone(), cte_plan);
    }

    // Build FROM clause
    let mut plan = build_from(&select.from, ctx)?;

    // Apply WHERE clause
    if let Some(ref where_clause) = select.where_clause {
        let predicate = build_expr(where_clause, &plan.schema())?;
        plan = Arc::new(LogicalOperator::Filter(FilterOperator {
            input: plan,
            predicate,
        }));
    }

    // Handle GROUP BY and aggregates
    let has_aggregates = select
        .columns
        .iter()
        .any(|item| matches!(&item.expr, AstExpr::Function(f) if is_aggregate_function(&f.name)));
    let has_group_by = !select.group_by.is_empty();

    if has_aggregates || has_group_by {
        plan = build_aggregate(select, plan, ctx)?;

        // Apply HAVING clause
        if let Some(ref having) = select.having {
            let predicate = build_expr(having, &plan.schema())?;
            plan = Arc::new(LogicalOperator::Filter(FilterOperator {
                input: plan,
                predicate,
            }));
        }
    }

    // Build projection
    plan = build_projection(select, plan, ctx)?;

    // Apply DISTINCT
    if select.distinct {
        plan = Arc::new(LogicalOperator::Distinct(DistinctOperator {
            input: plan,
            on_columns: None,
        }));
    }

    // Apply ORDER BY
    if !select.order_by.is_empty() {
        let order_by: PlanResult<Vec<_>> = select
            .order_by
            .iter()
            .map(|o| build_order_by(o, &plan.schema()))
            .collect();
        plan = Arc::new(LogicalOperator::Sort(SortOperator {
            input: plan,
            order_by: order_by?,
            fetch: select.limit.map(|l| l as usize),
        }));
    }

    // Apply LIMIT/OFFSET
    if select.limit.is_some() || select.offset.is_some() {
        plan = Arc::new(LogicalOperator::Limit(LimitOperator {
            input: plan,
            offset: select.offset.unwrap_or(0) as usize,
            fetch: select.limit.map(|l| l as usize),
        }));
    }

    Ok(plan)
}

fn build_from(from_items: &[FromItem], ctx: &mut PlanContext) -> PlanResult<Arc<LogicalOperator>> {
    if from_items.is_empty() {
        // SELECT without FROM
        return Ok(Arc::new(LogicalOperator::EmptyRelation(
            EmptyRelationOperator {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            },
        )));
    }

    let mut plan = build_from_item(&from_items[0], ctx)?;

    // Cross join remaining items
    for item in &from_items[1..] {
        let right = build_from_item(item, ctx)?;
        plan = Arc::new(LogicalOperator::Join(JoinOperator::new(
            plan,
            right,
            JoinType::Cross,
            None,
        )));
    }

    Ok(plan)
}

fn build_from_item(item: &FromItem, ctx: &mut PlanContext) -> PlanResult<Arc<LogicalOperator>> {
    match item {
        FromItem::Table(table_ref) => {
            let scan = ctx.resolve_table(table_ref)?;
            let mut op = Arc::new(LogicalOperator::Scan(scan));

            // Apply alias if present
            if let Some(ref alias) = table_ref.alias {
                let schema = op.schema();
                let aliased_fields: Vec<_> = schema
                    .fields()
                    .iter()
                    .map(|f| {
                        Field::new(
                            Column::qualified(alias, &f.column.name),
                            f.data_type.clone(),
                            f.nullable,
                        )
                    })
                    .collect();
                op = Arc::new(LogicalOperator::SubqueryAlias(SubqueryAliasOperator {
                    input: op,
                    alias: alias.clone(),
                    schema: Arc::new(Schema::new(aliased_fields)),
                }));
            }

            Ok(op)
        }
        FromItem::Join {
            left,
            join_type,
            right,
            on,
        } => {
            let left_plan = build_from_item(left, ctx)?;
            let right_plan = build_from_item(right, ctx)?;

            let merged_schema = left_plan.schema().merge(&right_plan.schema());
            let condition = on
                .as_ref()
                .map(|e| build_expr(e, &merged_schema))
                .transpose()?;

            let join_type = match join_type {
                parser::JoinType::Inner => JoinType::Inner,
                parser::JoinType::Left => JoinType::Left,
                parser::JoinType::Right => JoinType::Right,
                parser::JoinType::Full => JoinType::Full,
                parser::JoinType::Cross => JoinType::Cross,
            };

            Ok(Arc::new(LogicalOperator::Join(JoinOperator::new(
                left_plan, right_plan, join_type, condition,
            ))))
        }
        FromItem::Subquery { query, alias } => {
            let subquery_plan = build_select(query, ctx)?;
            let schema = subquery_plan.schema();

            let aliased_fields: Vec<_> = schema
                .fields()
                .iter()
                .map(|f| {
                    Field::new(
                        Column::qualified(alias, &f.column.name),
                        f.data_type.clone(),
                        f.nullable,
                    )
                })
                .collect();

            Ok(Arc::new(LogicalOperator::SubqueryAlias(
                SubqueryAliasOperator {
                    input: subquery_plan,
                    alias: alias.clone(),
                    schema: Arc::new(Schema::new(aliased_fields)),
                },
            )))
        }
    }
}

fn build_aggregate(
    select: &SelectStatement,
    input: Arc<LogicalOperator>,
    _ctx: &PlanContext,
) -> PlanResult<Arc<LogicalOperator>> {
    let input_schema = input.schema();

    // Build group by expressions
    let group_by: PlanResult<Vec<_>> = select
        .group_by
        .iter()
        .map(|e| build_expr(e, &input_schema))
        .collect();
    let group_by = group_by?;

    // Extract aggregate expressions from SELECT
    let mut aggregates = Vec::new();
    for item in &select.columns {
        extract_aggregates(&item.expr, &input_schema, &mut aggregates)?;
    }

    // Build output schema
    let mut fields = Vec::new();
    for expr in &group_by {
        let name = expr.output_name();
        let data_type = expr.data_type(&input_schema).unwrap_or(DataType::Int);
        fields.push(Field::nullable(&name, data_type));
    }
    for expr in &aggregates {
        let name = expr.output_name();
        let data_type = expr.data_type(&input_schema).unwrap_or(DataType::BigInt);
        fields.push(Field::nullable(&name, data_type));
    }

    Ok(Arc::new(LogicalOperator::Aggregate(AggregateOperator {
        input,
        group_by,
        aggregates,
        schema: Arc::new(Schema::new(fields)),
    })))
}

fn extract_aggregates(
    expr: &AstExpr,
    schema: &Schema,
    out: &mut Vec<LogicalExpr>,
) -> PlanResult<()> {
    match expr {
        AstExpr::Function(f) if is_aggregate_function(&f.name) => {
            let logical_expr = build_expr(expr, schema)?;
            if !out.iter().any(|e| *e == logical_expr) {
                out.push(logical_expr);
            }
        }
        AstExpr::BinaryOp { left, right, .. } => {
            extract_aggregates(left, schema, out)?;
            extract_aggregates(right, schema, out)?;
        }
        AstExpr::UnaryOp { expr, .. } => {
            extract_aggregates(expr, schema, out)?;
        }
        AstExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(op) = operand {
                extract_aggregates(op, schema, out)?;
            }
            for (w, t) in when_clauses {
                extract_aggregates(w, schema, out)?;
                extract_aggregates(t, schema, out)?;
            }
            if let Some(e) = else_clause {
                extract_aggregates(e, schema, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn build_projection(
    select: &SelectStatement,
    input: Arc<LogicalOperator>,
    _ctx: &PlanContext,
) -> PlanResult<Arc<LogicalOperator>> {
    let input_schema = input.schema();
    let mut exprs = Vec::new();
    let mut fields = Vec::new();

    for item in &select.columns {
        match &item.expr {
            AstExpr::Wildcard => {
                // Expand wildcard to all columns
                for field in input_schema.fields() {
                    exprs.push(LogicalExpr::Column(field.column.clone()));
                    fields.push(field.clone());
                }
            }
            AstExpr::QualifiedWildcard(table) => {
                // Expand qualified wildcard
                for field in input_schema.fields() {
                    if field.column.qualifier.as_deref() == Some(table) {
                        exprs.push(LogicalExpr::Column(field.column.clone()));
                        fields.push(field.clone());
                    }
                }
            }
            _ => {
                let expr = build_expr(&item.expr, &input_schema)?;
                let name = item.alias.clone().unwrap_or_else(|| expr.output_name());
                let data_type = expr.data_type(&input_schema).unwrap_or(DataType::Int);

                let expr = if item.alias.is_some() {
                    expr.alias(&name)
                } else {
                    expr
                };

                exprs.push(expr);
                fields.push(Field::nullable(name, data_type));
            }
        }
    }

    Ok(Arc::new(LogicalOperator::Projection(ProjectionOperator {
        input,
        exprs,
        schema: Arc::new(Schema::new(fields)),
    })))
}

fn build_insert(
    insert: &InsertStatement,
    ctx: &mut PlanContext,
) -> PlanResult<Arc<LogicalOperator>> {
    let scan = ctx.resolve_table(&insert.table)?;
    let table_schema = scan.table_schema;

    let values_plan = match &insert.values {
        InsertSource::Values(rows) => {
            let exprs: PlanResult<Vec<Vec<LogicalExpr>>> = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|e| build_expr(e, &Schema::empty()))
                        .collect()
                })
                .collect();

            Arc::new(LogicalOperator::Values(ValuesOperator {
                values: exprs?,
                schema: table_schema,
            }))
        }
        InsertSource::Query(query) => build_select(query, ctx)?,
        InsertSource::DefaultValues => Arc::new(LogicalOperator::Values(ValuesOperator {
            values: vec![vec![]],
            schema: table_schema,
        })),
    };

    Ok(values_plan)
}

fn build_update(
    update: &UpdateStatement,
    ctx: &mut PlanContext,
) -> PlanResult<Arc<LogicalOperator>> {
    let scan = ctx.resolve_table(&update.table)?;
    let mut plan = Arc::new(LogicalOperator::Scan(scan));

    if let Some(ref where_clause) = update.where_clause {
        let predicate = build_expr(where_clause, &plan.schema())?;
        plan = Arc::new(LogicalOperator::Filter(FilterOperator {
            input: plan,
            predicate,
        }));
    }

    Ok(plan)
}

fn build_delete(
    delete: &DeleteStatement,
    ctx: &mut PlanContext,
) -> PlanResult<Arc<LogicalOperator>> {
    let scan = ctx.resolve_table(&delete.table)?;
    let mut plan = Arc::new(LogicalOperator::Scan(scan));

    if let Some(ref where_clause) = delete.where_clause {
        let predicate = build_expr(where_clause, &plan.schema())?;
        plan = Arc::new(LogicalOperator::Filter(FilterOperator {
            input: plan,
            predicate,
        }));
    }

    Ok(plan)
}

fn build_expr(expr: &AstExpr, schema: &Schema) -> PlanResult<LogicalExpr> {
    match expr {
        AstExpr::Column(col_ref) => {
            let column = if let Some(ref table) = col_ref.table {
                Column::qualified(table, &col_ref.column)
            } else {
                Column::new(&col_ref.column)
            };
            Ok(LogicalExpr::Column(column))
        }
        AstExpr::Literal(lit) => Ok(LogicalExpr::Literal(lit.clone())),
        AstExpr::BinaryOp { left, op, right } => {
            let left = build_expr(left, schema)?;
            let right = build_expr(right, schema)?;
            let op = convert_binary_op(op)?;
            Ok(LogicalExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }
        AstExpr::UnaryOp { op, expr } => {
            let expr = build_expr(expr, schema)?;
            let op = convert_unary_op(op)?;
            Ok(LogicalExpr::UnaryOp {
                op,
                expr: Box::new(expr),
            })
        }
        AstExpr::IsNull(inner) => {
            let inner = build_expr(inner, schema)?;
            Ok(LogicalExpr::IsNull(Box::new(inner)))
        }
        AstExpr::IsNotNull(inner) => {
            let inner = build_expr(inner, schema)?;
            Ok(LogicalExpr::IsNotNull(Box::new(inner)))
        }
        AstExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let expr = build_expr(expr, schema)?;
            let low = build_expr(low, schema)?;
            let high = build_expr(high, schema)?;
            Ok(LogicalExpr::Between {
                expr: Box::new(expr),
                low: Box::new(low),
                high: Box::new(high),
                negated: *negated,
            })
        }
        AstExpr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = build_expr(expr, schema)?;
            let list: PlanResult<Vec<_>> = list.iter().map(|e| build_expr(e, schema)).collect();
            Ok(LogicalExpr::InList {
                expr: Box::new(expr),
                list: list?,
                negated: *negated,
            })
        }
        AstExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let operand = operand
                .as_ref()
                .map(|e| build_expr(e, schema))
                .transpose()?
                .map(Box::new);
            let when_then: PlanResult<Vec<_>> = when_clauses
                .iter()
                .map(|(w, t)| Ok((build_expr(w, schema)?, build_expr(t, schema)?)))
                .collect();
            let else_result = else_clause
                .as_ref()
                .map(|e| build_expr(e, schema))
                .transpose()?
                .map(Box::new);
            Ok(LogicalExpr::Case {
                operand,
                when_then: when_then?,
                else_result,
            })
        }
        AstExpr::Cast { expr, data_type } => {
            let expr = build_expr(expr, schema)?;
            Ok(LogicalExpr::Cast {
                expr: Box::new(expr),
                data_type: data_type.clone(),
            })
        }
        AstExpr::Function(func) => {
            let args: PlanResult<Vec<_>> =
                func.args.iter().map(|a| build_expr(a, schema)).collect();
            let args = args?;

            if is_aggregate_function(&func.name) {
                let agg_func = convert_aggregate_func(&func.name)?;

                // Handle COUNT(*) specially - convert to CountStar with no args
                let (agg_func, args) =
                    if matches!(agg_func, AggregateFunc::Count) && has_wildcard_arg(&args) {
                        (AggregateFunc::CountStar, vec![])
                    } else {
                        (agg_func, args)
                    };

                Ok(LogicalExpr::AggregateFunction {
                    name: agg_func,
                    args,
                    distinct: func.distinct,
                    filter: None,
                })
            } else {
                Ok(LogicalExpr::ScalarFunction {
                    name: func.name.clone(),
                    args,
                })
            }
        }
        AstExpr::Nested(inner) => build_expr(inner, schema),
        AstExpr::Wildcard => Ok(LogicalExpr::Wildcard),
        AstExpr::QualifiedWildcard(table) => Ok(LogicalExpr::QualifiedWildcard(table.clone())),
        _ => Err(PlanError::Unsupported(format!("Expression: {:?}", expr))),
    }
}

fn build_order_by(order: &AstOrderBy, schema: &Schema) -> PlanResult<SortExpr> {
    let expr = build_expr(&order.expr, schema)?;
    let asc = order.direction == parser::OrderDirection::Asc;
    let nulls_first = match order.nulls {
        Some(parser::NullsOrder::First) => true,
        Some(parser::NullsOrder::Last) => false,
        None => !asc, // Default: NULLS LAST for ASC, NULLS FIRST for DESC
    };
    Ok(SortExpr {
        expr,
        asc,
        nulls_first,
    })
}

fn convert_binary_op(op: &parser::BinaryOperator) -> PlanResult<BinaryOp> {
    match op {
        parser::BinaryOperator::Eq => Ok(BinaryOp::Eq),
        parser::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
        parser::BinaryOperator::Lt => Ok(BinaryOp::Lt),
        parser::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
        parser::BinaryOperator::Gt => Ok(BinaryOp::Gt),
        parser::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
        parser::BinaryOperator::Plus => Ok(BinaryOp::Plus),
        parser::BinaryOperator::Minus => Ok(BinaryOp::Minus),
        parser::BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
        parser::BinaryOperator::Divide => Ok(BinaryOp::Divide),
        parser::BinaryOperator::Modulo => Ok(BinaryOp::Modulo),
        parser::BinaryOperator::And => Ok(BinaryOp::And),
        parser::BinaryOperator::Or => Ok(BinaryOp::Or),
        parser::BinaryOperator::Like => Ok(BinaryOp::Like),
        parser::BinaryOperator::ILike => Ok(BinaryOp::ILike),
        parser::BinaryOperator::Concat => Ok(BinaryOp::Concat),
        parser::BinaryOperator::BitwiseAnd => Ok(BinaryOp::BitwiseAnd),
        parser::BinaryOperator::BitwiseOr => Ok(BinaryOp::BitwiseOr),
        parser::BinaryOperator::BitwiseXor => Ok(BinaryOp::BitwiseXor),
        _ => Err(PlanError::Unsupported(format!("Binary operator: {:?}", op))),
    }
}

fn convert_unary_op(op: &parser::UnaryOperator) -> PlanResult<UnaryOp> {
    match op {
        parser::UnaryOperator::Not => Ok(UnaryOp::Not),
        parser::UnaryOperator::Minus => Ok(UnaryOp::Minus),
        parser::UnaryOperator::Plus => Ok(UnaryOp::Plus),
        parser::UnaryOperator::BitwiseNot => Ok(UnaryOp::BitwiseNot),
    }
}

fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "FIRST"
            | "LAST"
            | "STRING_AGG"
            | "ARRAY_AGG"
            | "BOOL_AND"
            | "BOOL_OR"
    )
}

/// Checks if the argument list contains a wildcard expression.
fn has_wildcard_arg(args: &[LogicalExpr]) -> bool {
    args.iter()
        .any(|a| matches!(a, LogicalExpr::Wildcard | LogicalExpr::QualifiedWildcard(_)))
}

fn convert_aggregate_func(name: &str) -> PlanResult<AggregateFunc> {
    match name.to_uppercase().as_str() {
        "COUNT" => Ok(AggregateFunc::Count),
        "SUM" => Ok(AggregateFunc::Sum),
        "AVG" => Ok(AggregateFunc::Avg),
        "MIN" => Ok(AggregateFunc::Min),
        "MAX" => Ok(AggregateFunc::Max),
        "FIRST" => Ok(AggregateFunc::First),
        "LAST" => Ok(AggregateFunc::Last),
        "STRING_AGG" => Ok(AggregateFunc::StringAgg),
        "ARRAY_AGG" => Ok(AggregateFunc::ArrayAgg),
        "BOOL_AND" => Ok(AggregateFunc::BoolAnd),
        "BOOL_OR" => Ok(AggregateFunc::BoolOr),
        _ => Err(PlanError::Unsupported(format!(
            "Aggregate function: {}",
            name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    fn test_catalog() -> MemoryCatalog {
        let mut catalog = MemoryCatalog::new();
        catalog.add_table(TableMeta::new(
            "users",
            Schema::new(vec![
                Field::not_null("id", DataType::Int),
                Field::nullable("name", DataType::Varchar(Some(255))),
                Field::nullable("email", DataType::Text),
                Field::not_null("active", DataType::Boolean),
            ]),
        ));
        catalog.add_table(TableMeta::new(
            "orders",
            Schema::new(vec![
                Field::not_null("id", DataType::Int),
                Field::not_null("user_id", DataType::Int),
                Field::nullable(
                    "total",
                    DataType::Decimal {
                        precision: Some(10),
                        scale: Some(2),
                    },
                ),
                Field::not_null("created_at", DataType::Timestamp),
            ]),
        ));
        catalog
    }

    #[test]
    fn test_simple_select() {
        let catalog = test_catalog();
        let stmt = Parser::parse_one("SELECT id, name FROM users").unwrap();
        let plan = build_plan(&stmt, &catalog).unwrap();

        let display = plan.display();
        assert!(display.contains("Projection"));
        assert!(display.contains("Scan: users"));
    }

    #[test]
    fn test_select_with_where() {
        let catalog = test_catalog();
        let stmt = Parser::parse_one("SELECT * FROM users WHERE active = true").unwrap();
        let plan = build_plan(&stmt, &catalog).unwrap();

        let display = plan.display();
        assert!(display.contains("Filter"));
        assert!(display.contains("Scan: users"));
    }

    #[test]
    fn test_join() {
        let catalog = test_catalog();
        let stmt = Parser::parse_one(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
        let plan = build_plan(&stmt, &catalog).unwrap();

        let display = plan.display();
        assert!(display.contains("Join"));
    }

    #[test]
    fn test_aggregate() {
        let catalog = test_catalog();
        let stmt =
            Parser::parse_one("SELECT user_id, SUM(total) FROM orders GROUP BY user_id").unwrap();
        let plan = build_plan(&stmt, &catalog).unwrap();

        let display = plan.display();
        assert!(display.contains("Aggregate"));
    }

    #[test]
    fn test_order_by_limit() {
        let catalog = test_catalog();
        let stmt =
            Parser::parse_one("SELECT * FROM users ORDER BY id DESC LIMIT 10 OFFSET 5").unwrap();
        let plan = build_plan(&stmt, &catalog).unwrap();

        let display = plan.display();
        assert!(display.contains("Sort"));
        assert!(display.contains("Limit"));
    }

    #[test]
    fn test_subquery() {
        let catalog = test_catalog();
        let stmt =
            Parser::parse_one("SELECT * FROM (SELECT id, name FROM users) AS t WHERE id > 5")
                .unwrap();
        let plan = build_plan(&stmt, &catalog).unwrap();

        let display = plan.display();
        assert!(display.contains("SubqueryAlias: t"));
    }

    #[test]
    fn test_explain() {
        let catalog = test_catalog();
        let stmt = Parser::parse_one("SELECT id FROM users WHERE active = true").unwrap();
        let plan = build_plan(&stmt, &catalog).unwrap();

        let explain = plan.explain();
        assert!(explain.contains("Schema:"));
    }
}
