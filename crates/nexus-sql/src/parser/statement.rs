//! SQL Statement parsing and representation.
//!
//! This module defines all SQL statement types supported by NexusDB.

use serde::{Deserialize, Serialize};
use sqlparser::ast as sql_ast;

use super::{ColumnRef, DataType, Expr, JoinType, OrderByExpr, ParseError, ParseResult, TableRef};

/// A parsed SQL statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    /// SELECT query.
    Select(SelectStatement),
    /// INSERT statement.
    Insert(InsertStatement),
    /// UPDATE statement.
    Update(UpdateStatement),
    /// DELETE statement.
    Delete(DeleteStatement),
    /// CREATE TABLE statement.
    CreateTable(CreateTableStatement),
    /// DROP TABLE statement.
    DropTable(DropTableStatement),
    /// ALTER TABLE statement.
    AlterTable(AlterTableStatement),
    /// CREATE INDEX statement.
    CreateIndex(CreateIndexStatement),
    /// DROP INDEX statement.
    DropIndex(DropIndexStatement),
    /// BEGIN transaction.
    Begin,
    /// COMMIT transaction.
    Commit,
    /// ROLLBACK transaction.
    Rollback,
    /// EXPLAIN query.
    Explain(Box<Statement>),
    /// EXPLAIN ANALYZE query.
    ExplainAnalyze(Box<Statement>),
}

impl Statement {
    /// Converts from sqlparser's Statement.
    pub fn from_sql_ast(stmt: sql_ast::Statement) -> ParseResult<Self> {
        match stmt {
            sql_ast::Statement::Query(query) => {
                Ok(Statement::Select(SelectStatement::from_sql_ast(*query)?))
            }
            sql_ast::Statement::Insert {
                table_name,
                columns,
                source,
                returning,
                ..
            } => Ok(Statement::Insert(InsertStatement::from_parts(
                table_name, columns, source, returning,
            )?)),
            sql_ast::Statement::Update {
                table,
                assignments,
                from,
                selection,
                ..
            } => Ok(Statement::Update(UpdateStatement::from_parts(
                table,
                assignments,
                from,
                selection,
            )?)),
            sql_ast::Statement::Delete {
                from,
                selection,
                returning,
                ..
            } => Ok(Statement::Delete(DeleteStatement::from_parts(
                from, selection, returning,
            )?)),
            sql_ast::Statement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
                ..
            } => Ok(Statement::CreateTable(CreateTableStatement::from_parts(
                name,
                columns,
                constraints,
                if_not_exists,
            )?)),
            sql_ast::Statement::Drop {
                object_type: sql_ast::ObjectType::Table,
                if_exists,
                names,
                cascade,
                ..
            } => Ok(Statement::DropTable(DropTableStatement {
                names: names
                    .into_iter()
                    .map(|n| table_ref_from_object_name(&n))
                    .collect(),
                if_exists,
                cascade,
            })),
            sql_ast::Statement::AlterTable {
                name,
                if_exists,
                operations,
                ..
            } => Ok(Statement::AlterTable(AlterTableStatement::from_parts(
                name, if_exists, operations,
            )?)),
            sql_ast::Statement::CreateIndex {
                name,
                table_name,
                columns,
                unique,
                if_not_exists,
                ..
            } => Ok(Statement::CreateIndex(CreateIndexStatement::from_parts(
                name,
                table_name,
                columns,
                unique,
                if_not_exists,
            )?)),
            sql_ast::Statement::Drop {
                object_type: sql_ast::ObjectType::Index,
                if_exists,
                names,
                ..
            } => Ok(Statement::DropIndex(DropIndexStatement {
                names: names.iter().map(|n| n.to_string()).collect(),
                if_exists,
            })),
            sql_ast::Statement::StartTransaction { .. } => Ok(Statement::Begin),
            sql_ast::Statement::Commit { .. } => Ok(Statement::Commit),
            sql_ast::Statement::Rollback { .. } => Ok(Statement::Rollback),
            sql_ast::Statement::Explain {
                statement, analyze, ..
            } => {
                let inner = Statement::from_sql_ast(*statement)?;
                if analyze {
                    Ok(Statement::ExplainAnalyze(Box::new(inner)))
                } else {
                    Ok(Statement::Explain(Box::new(inner)))
                }
            }
            _ => Err(ParseError::Unsupported(format!("Statement: {:?}", stmt))),
        }
    }

    /// Returns true if this is a read-only statement.
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            Statement::Select(_) | Statement::Explain(_) | Statement::ExplainAnalyze(_)
        )
    }

    /// Returns true if this is a DDL statement.
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            Statement::CreateTable(_)
                | Statement::DropTable(_)
                | Statement::AlterTable(_)
                | Statement::CreateIndex(_)
                | Statement::DropIndex(_)
        )
    }

    /// Returns true if this is a transaction control statement.
    pub fn is_txn_control(&self) -> bool {
        matches!(
            self,
            Statement::Begin | Statement::Commit | Statement::Rollback
        )
    }
}

/// SELECT statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    /// WITH clause (Common Table Expressions).
    pub ctes: Vec<CTE>,
    /// Whether DISTINCT is specified.
    pub distinct: bool,
    /// Selected columns/expressions.
    pub columns: Vec<SelectItem>,
    /// FROM clause tables.
    pub from: Vec<FromItem>,
    /// WHERE clause.
    pub where_clause: Option<Expr>,
    /// GROUP BY expressions.
    pub group_by: Vec<Expr>,
    /// HAVING clause.
    pub having: Option<Expr>,
    /// ORDER BY clauses.
    pub order_by: Vec<OrderByExpr>,
    /// LIMIT clause.
    pub limit: Option<u64>,
    /// OFFSET clause.
    pub offset: Option<u64>,
}

impl SelectStatement {
    /// Creates a new SELECT statement.
    pub fn new() -> Self {
        Self {
            ctes: Vec::new(),
            distinct: false,
            columns: Vec::new(),
            from: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    /// Converts from sqlparser's Query.
    pub fn from_sql_ast(query: sql_ast::Query) -> ParseResult<Self> {
        // Parse CTEs
        let ctes: ParseResult<Vec<_>> = query
            .with
            .map(|w| w.cte_tables.into_iter().map(CTE::from_sql_ast).collect())
            .unwrap_or_else(|| Ok(Vec::new()));

        // Parse the main SELECT
        match *query.body {
            sql_ast::SetExpr::Select(select) => {
                let columns: ParseResult<Vec<_>> = select
                    .projection
                    .into_iter()
                    .map(SelectItem::from_sql_ast)
                    .collect();

                let from: ParseResult<Vec<_>> = select
                    .from
                    .into_iter()
                    .map(FromItem::from_sql_ast)
                    .collect();

                let where_clause = select.selection.map(Expr::from_sql_ast).transpose()?;

                let group_by: ParseResult<Vec<_>> = match select.group_by {
                    sql_ast::GroupByExpr::Expressions(exprs) => {
                        exprs.into_iter().map(Expr::from_sql_ast).collect()
                    }
                    sql_ast::GroupByExpr::All => {
                        return Err(ParseError::Unsupported("GROUP BY ALL".to_string()))
                    }
                };

                let having = select.having.map(Expr::from_sql_ast).transpose()?;

                let order_by: ParseResult<Vec<_>> = query
                    .order_by
                    .into_iter()
                    .map(OrderByExpr::from_sql_ast)
                    .collect();

                let limit = query.limit.and_then(|e| extract_limit(&e));
                let offset = query.offset.and_then(|o| extract_limit(&o.value));

                Ok(Self {
                    ctes: ctes?,
                    distinct: select.distinct.is_some(),
                    columns: columns?,
                    from: from?,
                    where_clause,
                    group_by: group_by?,
                    having,
                    order_by: order_by?,
                    limit,
                    offset,
                })
            }
            _ => Err(ParseError::Unsupported(
                "Non-SELECT set expression".to_string(),
            )),
        }
    }
}

impl Default for SelectStatement {
    fn default() -> Self {
        Self::new()
    }
}

/// A selected item (column or expression).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectItem {
    /// The expression.
    pub expr: Expr,
    /// Optional alias.
    pub alias: Option<String>,
}

impl SelectItem {
    /// Creates a new select item.
    pub fn new(expr: Expr) -> Self {
        Self { expr, alias: None }
    }

    /// Creates a select item with an alias.
    pub fn with_alias(expr: Expr, alias: impl Into<String>) -> Self {
        Self {
            expr,
            alias: Some(alias.into()),
        }
    }

    /// Converts from sqlparser's SelectItem.
    pub fn from_sql_ast(item: sql_ast::SelectItem) -> ParseResult<Self> {
        match item {
            sql_ast::SelectItem::UnnamedExpr(expr) => Ok(Self {
                expr: Expr::from_sql_ast(expr)?,
                alias: None,
            }),
            sql_ast::SelectItem::ExprWithAlias { expr, alias } => Ok(Self {
                expr: Expr::from_sql_ast(expr)?,
                alias: Some(alias.value),
            }),
            sql_ast::SelectItem::Wildcard(_) => Ok(Self {
                expr: Expr::Wildcard,
                alias: None,
            }),
            sql_ast::SelectItem::QualifiedWildcard(name, _) => Ok(Self {
                expr: Expr::QualifiedWildcard(name.to_string()),
                alias: None,
            }),
        }
    }
}

/// A FROM clause item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FromItem {
    /// A table reference.
    Table(TableRef),
    /// A joined table.
    Join {
        /// Left side.
        left: Box<FromItem>,
        /// Join type.
        join_type: JoinType,
        /// Right side.
        right: Box<FromItem>,
        /// Join condition.
        on: Option<Expr>,
    },
    /// A subquery.
    Subquery {
        /// The subquery.
        query: Box<SelectStatement>,
        /// Alias.
        alias: String,
    },
}

impl FromItem {
    /// Creates a table reference.
    pub fn table(name: impl Into<String>) -> Self {
        FromItem::Table(TableRef::new(name))
    }

    /// Converts from sqlparser's TableWithJoins.
    pub fn from_sql_ast(table: sql_ast::TableWithJoins) -> ParseResult<Self> {
        let mut result = from_table_factor(table.relation)?;

        for join in table.joins {
            let right = from_table_factor(join.relation)?;
            let join_type = JoinType::from_sql_ast(&join.join_operator)?;
            let on = match &join.join_operator {
                sql_ast::JoinOperator::Inner(constraint)
                | sql_ast::JoinOperator::LeftOuter(constraint)
                | sql_ast::JoinOperator::RightOuter(constraint)
                | sql_ast::JoinOperator::FullOuter(constraint) => match constraint {
                    sql_ast::JoinConstraint::On(expr) => Some(Expr::from_sql_ast(expr.clone())?),
                    _ => None,
                },
                _ => None,
            };

            result = FromItem::Join {
                left: Box::new(result),
                join_type,
                right: Box::new(right),
                on,
            };
        }

        Ok(result)
    }
}

/// Converts a TableFactor to FromItem.
fn from_table_factor(factor: sql_ast::TableFactor) -> ParseResult<FromItem> {
    match factor {
        sql_ast::TableFactor::Table { name, alias, .. } => {
            let mut table_ref = table_ref_from_object_name(&name);
            if let Some(a) = alias {
                table_ref.alias = Some(a.name.value);
            }
            Ok(FromItem::Table(table_ref))
        }
        sql_ast::TableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias = alias
                .ok_or_else(|| ParseError::Syntax("Subquery requires alias".to_string()))?
                .name
                .value;
            Ok(FromItem::Subquery {
                query: Box::new(SelectStatement::from_sql_ast(*subquery)?),
                alias,
            })
        }
        sql_ast::TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let mut result = FromItem::from_sql_ast(*table_with_joins)?;
            if let Some(a) = alias {
                // Apply alias to the result if it's a table
                if let FromItem::Table(ref mut t) = result {
                    t.alias = Some(a.name.value);
                }
            }
            Ok(result)
        }
        _ => Err(ParseError::Unsupported(format!(
            "Table factor: {:?}",
            factor
        ))),
    }
}

/// Converts an ObjectName to TableRef.
fn table_ref_from_object_name(name: &sql_ast::ObjectName) -> TableRef {
    let parts: Vec<_> = name.0.iter().map(|i| i.value.clone()).collect();
    match parts.len() {
        1 => TableRef::new(&parts[0]),
        2 => TableRef::with_schema(&parts[0], &parts[1]),
        _ => TableRef::new(name.to_string()),
    }
}

/// Common Table Expression (WITH clause).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CTE {
    /// Name of the CTE.
    pub name: String,
    /// Column aliases.
    pub columns: Vec<String>,
    /// The query.
    pub query: SelectStatement,
}

impl CTE {
    /// Converts from sqlparser's Cte.
    pub fn from_sql_ast(cte: sql_ast::Cte) -> ParseResult<Self> {
        Ok(Self {
            name: cte.alias.name.value,
            columns: cte.alias.columns.into_iter().map(|c| c.value).collect(),
            query: SelectStatement::from_sql_ast(*cte.query)?,
        })
    }
}

/// INSERT statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    /// Target table.
    pub table: TableRef,
    /// Column names (if specified).
    pub columns: Vec<String>,
    /// Values to insert.
    pub values: InsertSource,
    /// ON CONFLICT clause.
    pub on_conflict: Option<OnConflict>,
    /// RETURNING clause.
    pub returning: Vec<SelectItem>,
}

impl InsertStatement {
    /// Converts from sqlparser's Insert parts.
    pub fn from_parts(
        table_name: sql_ast::ObjectName,
        columns: Vec<sql_ast::Ident>,
        source: Option<Box<sql_ast::Query>>,
        returning: Option<Vec<sql_ast::SelectItem>>,
    ) -> ParseResult<Self> {
        let table = table_ref_from_object_name(&table_name);
        let columns: Vec<_> = columns.into_iter().map(|c| c.value).collect();

        let values = match source {
            Some(source) => InsertSource::from_query(*source)?,
            None => return Err(ParseError::Syntax("INSERT without values".to_string())),
        };

        let returning: ParseResult<Vec<_>> = returning
            .unwrap_or_default()
            .into_iter()
            .map(SelectItem::from_sql_ast)
            .collect();

        Ok(Self {
            table,
            columns,
            values,
            on_conflict: None, // TODO: Parse ON CONFLICT
            returning: returning?,
        })
    }
}

/// Source of values for INSERT.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InsertSource {
    /// VALUES clause with literal values.
    Values(Vec<Vec<Expr>>),
    /// SELECT subquery.
    Query(Box<SelectStatement>),
    /// DEFAULT VALUES.
    DefaultValues,
}

impl InsertSource {
    /// Converts from a Query source.
    fn from_query(query: sql_ast::Query) -> ParseResult<Self> {
        match *query.body {
            sql_ast::SetExpr::Values(values) => {
                let rows: ParseResult<Vec<Vec<Expr>>> = values
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().map(Expr::from_sql_ast).collect())
                    .collect();
                Ok(InsertSource::Values(rows?))
            }
            sql_ast::SetExpr::Select(_) => Ok(InsertSource::Query(Box::new(
                SelectStatement::from_sql_ast(query)?,
            ))),
            _ => Err(ParseError::Unsupported("INSERT source".to_string())),
        }
    }
}

/// ON CONFLICT clause for INSERT.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnConflict {
    /// Conflict target columns.
    pub target: Vec<String>,
    /// Action to take on conflict.
    pub action: ConflictAction,
}

/// Action to take on INSERT conflict.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConflictAction {
    /// DO NOTHING.
    DoNothing,
    /// DO UPDATE SET.
    DoUpdate(Vec<Assignment>),
}

/// UPDATE statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateStatement {
    /// Target table.
    pub table: TableRef,
    /// Assignments.
    pub assignments: Vec<Assignment>,
    /// FROM clause (for UPDATE ... FROM).
    pub from: Vec<FromItem>,
    /// WHERE clause.
    pub where_clause: Option<Expr>,
    /// RETURNING clause.
    pub returning: Vec<SelectItem>,
}

impl UpdateStatement {
    /// Converts from parts.
    fn from_parts(
        table: sql_ast::TableWithJoins,
        assignments: Vec<sql_ast::Assignment>,
        from: Option<sql_ast::TableWithJoins>,
        selection: Option<sql_ast::Expr>,
    ) -> ParseResult<Self> {
        let table_ref = match from_table_factor(table.relation)? {
            FromItem::Table(t) => t,
            _ => return Err(ParseError::Syntax("UPDATE requires table".to_string())),
        };

        let assignments: ParseResult<Vec<_>> = assignments
            .into_iter()
            .map(Assignment::from_sql_ast)
            .collect();

        let from_items: ParseResult<Vec<_>> =
            from.into_iter().map(FromItem::from_sql_ast).collect();

        let where_clause = selection.map(Expr::from_sql_ast).transpose()?;

        Ok(Self {
            table: table_ref,
            assignments: assignments?,
            from: from_items?,
            where_clause,
            returning: Vec::new(),
        })
    }
}

/// An assignment (column = value).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    /// Column being assigned.
    pub column: ColumnRef,
    /// Value being assigned.
    pub value: Expr,
}

impl Assignment {
    /// Converts from sqlparser's Assignment.
    pub fn from_sql_ast(assign: sql_ast::Assignment) -> ParseResult<Self> {
        let column = if assign.id.len() == 1 {
            ColumnRef::new(&assign.id[0].value)
        } else if assign.id.len() > 1 {
            ColumnRef::qualified(&assign.id[0].value, &assign.id[assign.id.len() - 1].value)
        } else {
            return Err(ParseError::Syntax("Empty assignment target".to_string()));
        };

        Ok(Self {
            column,
            value: Expr::from_sql_ast(assign.value)?,
        })
    }
}

/// DELETE statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
    /// Target table.
    pub table: TableRef,
    /// WHERE clause.
    pub where_clause: Option<Expr>,
    /// RETURNING clause.
    pub returning: Vec<SelectItem>,
}

impl DeleteStatement {
    /// Converts from sqlparser's Delete parts.
    pub fn from_parts(
        from: Vec<sql_ast::TableWithJoins>,
        selection: Option<sql_ast::Expr>,
        returning: Option<Vec<sql_ast::SelectItem>>,
    ) -> ParseResult<Self> {
        let table = from
            .first()
            .ok_or_else(|| ParseError::Syntax("DELETE requires FROM".to_string()))?;

        let table_ref = match from_table_factor(table.relation.clone())? {
            FromItem::Table(t) => t,
            _ => return Err(ParseError::Syntax("DELETE requires table".to_string())),
        };

        let where_clause = selection.map(Expr::from_sql_ast).transpose()?;

        let returning: ParseResult<Vec<_>> = returning
            .unwrap_or_default()
            .into_iter()
            .map(SelectItem::from_sql_ast)
            .collect();

        Ok(Self {
            table: table_ref,
            where_clause,
            returning: returning?,
        })
    }
}

/// CREATE TABLE statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    /// Table name.
    pub name: TableRef,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Table constraints.
    pub constraints: Vec<TableConstraint>,
    /// IF NOT EXISTS.
    pub if_not_exists: bool,
}

impl CreateTableStatement {
    /// Converts from sqlparser's CreateTable parts.
    pub fn from_parts(
        name: sql_ast::ObjectName,
        columns: Vec<sql_ast::ColumnDef>,
        constraints: Vec<sql_ast::TableConstraint>,
        if_not_exists: bool,
    ) -> ParseResult<Self> {
        let columns: ParseResult<Vec<_>> =
            columns.into_iter().map(ColumnDef::from_sql_ast).collect();

        let constraints: ParseResult<Vec<_>> = constraints
            .into_iter()
            .map(TableConstraint::from_sql_ast)
            .collect();

        Ok(Self {
            name: table_ref_from_object_name(&name),
            columns: columns?,
            constraints: constraints?,
            if_not_exists,
        })
    }
}

/// Column definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Data type.
    pub data_type: DataType,
    /// Whether NULL is allowed.
    pub nullable: bool,
    /// Default value.
    pub default: Option<Expr>,
    /// Column constraints.
    pub constraints: Vec<ColumnConstraint>,
}

impl ColumnDef {
    /// Converts from sqlparser's ColumnDef.
    pub fn from_sql_ast(col: sql_ast::ColumnDef) -> ParseResult<Self> {
        let data_type = DataType::from_sql_ast(&col.data_type)?;
        let mut nullable = true;
        let mut default = None;
        let mut constraints = Vec::new();

        for opt in col.options {
            match opt.option {
                sql_ast::ColumnOption::Null => nullable = true,
                sql_ast::ColumnOption::NotNull => nullable = false,
                sql_ast::ColumnOption::Default(expr) => {
                    default = Some(Expr::from_sql_ast(expr)?);
                }
                sql_ast::ColumnOption::Unique { is_primary, .. } => {
                    if is_primary {
                        constraints.push(ColumnConstraint::PrimaryKey);
                        nullable = false;
                    } else {
                        constraints.push(ColumnConstraint::Unique);
                    }
                }
                sql_ast::ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    constraints.push(ColumnConstraint::References {
                        table: foreign_table.to_string(),
                        column: referred_columns.first().map(|c| c.value.clone()),
                    });
                }
                sql_ast::ColumnOption::Check(expr) => {
                    constraints.push(ColumnConstraint::Check(Expr::from_sql_ast(expr)?));
                }
                _ => {}
            }
        }

        Ok(Self {
            name: col.name.value,
            data_type,
            nullable,
            default,
            constraints,
        })
    }
}

/// Column constraint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnConstraint {
    /// PRIMARY KEY.
    PrimaryKey,
    /// UNIQUE.
    Unique,
    /// REFERENCES (foreign key).
    References {
        /// Referenced table.
        table: String,
        /// Referenced column.
        column: Option<String>,
    },
    /// CHECK constraint.
    Check(Expr),
}

/// Table constraint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableConstraint {
    /// PRIMARY KEY.
    PrimaryKey {
        /// Constraint name.
        name: Option<String>,
        /// Columns.
        columns: Vec<String>,
    },
    /// UNIQUE.
    Unique {
        /// Constraint name.
        name: Option<String>,
        /// Columns.
        columns: Vec<String>,
    },
    /// FOREIGN KEY.
    ForeignKey {
        /// Constraint name.
        name: Option<String>,
        /// Local columns.
        columns: Vec<String>,
        /// Referenced table.
        ref_table: String,
        /// Referenced columns.
        ref_columns: Vec<String>,
    },
    /// CHECK constraint.
    Check {
        /// Constraint name.
        name: Option<String>,
        /// Check expression.
        expr: Expr,
    },
}

impl TableConstraint {
    /// Converts from sqlparser's TableConstraint.
    pub fn from_sql_ast(constraint: sql_ast::TableConstraint) -> ParseResult<Self> {
        match constraint {
            sql_ast::TableConstraint::Unique {
                name,
                columns,
                is_primary,
                ..
            } => {
                let cols: Vec<_> = columns.into_iter().map(|c| c.value).collect();
                if is_primary {
                    Ok(TableConstraint::PrimaryKey {
                        name: name.map(|n| n.value),
                        columns: cols,
                    })
                } else {
                    Ok(TableConstraint::Unique {
                        name: name.map(|n| n.value),
                        columns: cols,
                    })
                }
            }
            sql_ast::TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                ..
            } => Ok(TableConstraint::ForeignKey {
                name: name.map(|n| n.value),
                columns: columns.into_iter().map(|c| c.value).collect(),
                ref_table: foreign_table.to_string(),
                ref_columns: referred_columns.into_iter().map(|c| c.value).collect(),
            }),
            sql_ast::TableConstraint::Check { name, expr } => Ok(TableConstraint::Check {
                name: name.map(|n| n.value),
                expr: Expr::from_sql_ast(*expr)?,
            }),
            _ => Err(ParseError::Unsupported(format!(
                "Constraint: {:?}",
                constraint
            ))),
        }
    }
}

/// DROP TABLE statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTableStatement {
    /// Table names.
    pub names: Vec<TableRef>,
    /// IF EXISTS.
    pub if_exists: bool,
    /// CASCADE.
    pub cascade: bool,
}

/// ALTER TABLE statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStatement {
    /// Table name.
    pub table: TableRef,
    /// IF EXISTS.
    pub if_exists: bool,
    /// Alterations.
    pub operations: Vec<AlterOperation>,
}

impl AlterTableStatement {
    /// Converts from parts.
    fn from_parts(
        name: sql_ast::ObjectName,
        if_exists: bool,
        operations: Vec<sql_ast::AlterTableOperation>,
    ) -> ParseResult<Self> {
        let ops: ParseResult<Vec<_>> = operations
            .into_iter()
            .map(AlterOperation::from_sql_ast)
            .collect();

        Ok(Self {
            table: table_ref_from_object_name(&name),
            if_exists,
            operations: ops?,
        })
    }
}

/// ALTER TABLE operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterOperation {
    /// ADD COLUMN.
    AddColumn(ColumnDef),
    /// DROP COLUMN.
    DropColumn {
        /// Column name.
        name: String,
        /// IF EXISTS.
        if_exists: bool,
    },
    /// ALTER COLUMN.
    AlterColumn {
        /// Column name.
        name: String,
        /// New data type.
        data_type: Option<DataType>,
        /// Set/drop NOT NULL.
        set_not_null: Option<bool>,
        /// Set default.
        set_default: Option<Expr>,
    },
    /// RENAME COLUMN.
    RenameColumn {
        /// Old name.
        old_name: String,
        /// New name.
        new_name: String,
    },
    /// RENAME TABLE.
    RenameTable(String),
}

impl AlterOperation {
    /// Converts from sqlparser's AlterTableOperation.
    pub fn from_sql_ast(op: sql_ast::AlterTableOperation) -> ParseResult<Self> {
        match op {
            sql_ast::AlterTableOperation::AddColumn { column_def, .. } => Ok(
                AlterOperation::AddColumn(ColumnDef::from_sql_ast(column_def)?),
            ),
            sql_ast::AlterTableOperation::DropColumn {
                column_name,
                if_exists,
                ..
            } => Ok(AlterOperation::DropColumn {
                name: column_name.value,
                if_exists,
            }),
            sql_ast::AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => Ok(AlterOperation::RenameColumn {
                old_name: old_column_name.value,
                new_name: new_column_name.value,
            }),
            sql_ast::AlterTableOperation::RenameTable { table_name } => {
                Ok(AlterOperation::RenameTable(table_name.to_string()))
            }
            _ => Err(ParseError::Unsupported(format!(
                "ALTER operation: {:?}",
                op
            ))),
        }
    }
}

/// CREATE INDEX statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateIndexStatement {
    /// Index name.
    pub name: String,
    /// Table name.
    pub table: TableRef,
    /// Column expressions.
    pub columns: Vec<OrderByExpr>,
    /// Whether the index is unique.
    pub unique: bool,
    /// IF NOT EXISTS.
    pub if_not_exists: bool,
}

impl CreateIndexStatement {
    /// Converts from sqlparser's CreateIndex parts.
    pub fn from_parts(
        name: Option<sql_ast::ObjectName>,
        table_name: sql_ast::ObjectName,
        columns: Vec<sql_ast::OrderByExpr>,
        unique: bool,
        if_not_exists: bool,
    ) -> ParseResult<Self> {
        let columns: ParseResult<Vec<_>> = columns
            .into_iter()
            .map(|c| {
                Ok(OrderByExpr {
                    expr: Expr::from_sql_ast(c.expr)?,
                    direction: if c.asc.unwrap_or(true) {
                        super::OrderDirection::Asc
                    } else {
                        super::OrderDirection::Desc
                    },
                    nulls: c.nulls_first.map(|f| {
                        if f {
                            super::NullsOrder::First
                        } else {
                            super::NullsOrder::Last
                        }
                    }),
                })
            })
            .collect();

        Ok(Self {
            name: name.map(|n| n.to_string()).unwrap_or_default(),
            table: table_ref_from_object_name(&table_name),
            columns: columns?,
            unique,
            if_not_exists,
        })
    }
}

/// DROP INDEX statement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropIndexStatement {
    /// Index names.
    pub names: Vec<String>,
    /// IF EXISTS.
    pub if_exists: bool,
}

/// Extracts a numeric limit from an expression.
fn extract_limit(expr: &sql_ast::Expr) -> Option<u64> {
    match expr {
        sql_ast::Expr::Value(sql_ast::Value::Number(n, _)) => n.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    #[test]
    fn test_select_statement() {
        let sql = "SELECT id, name FROM users WHERE active = true ORDER BY id LIMIT 10";
        let stmt = Parser::parse_one(sql).unwrap();

        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.columns.len(), 2);
                assert_eq!(select.from.len(), 1);
                assert!(select.where_clause.is_some());
                assert_eq!(select.order_by.len(), 1);
                assert_eq!(select.limit, Some(10));
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_insert_statement() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
        let stmt = Parser::parse_one(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.table.table, "users");
                assert_eq!(insert.columns.len(), 2);
                if let InsertSource::Values(rows) = &insert.values {
                    assert_eq!(rows.len(), 2);
                } else {
                    panic!("Expected VALUES");
                }
            }
            _ => panic!("Expected INSERT"),
        }
    }

    #[test]
    fn test_update_statement() {
        let sql = "UPDATE users SET name = 'Charlie' WHERE id = 1";
        let stmt = Parser::parse_one(sql).unwrap();

        match stmt {
            Statement::Update(update) => {
                assert_eq!(update.table.table, "users");
                assert_eq!(update.assignments.len(), 1);
                assert!(update.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE"),
        }
    }

    #[test]
    fn test_delete_statement() {
        let sql = "DELETE FROM users WHERE id = 1";
        let stmt = Parser::parse_one(sql).unwrap();

        match stmt {
            Statement::Delete(delete) => {
                assert_eq!(delete.table.table, "users");
                assert!(delete.where_clause.is_some());
            }
            _ => panic!("Expected DELETE"),
        }
    }

    #[test]
    fn test_create_table() {
        let sql = r#"
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        "#;
        let stmt = Parser::parse_one(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.name.table, "users");
                assert_eq!(create.columns.len(), 4);
                assert!(!create.columns[0].nullable); // PRIMARY KEY implies NOT NULL
                assert!(!create.columns[1].nullable);
            }
            _ => panic!("Expected CREATE TABLE"),
        }
    }

    #[test]
    fn test_statement_predicates() {
        let select = Statement::Select(SelectStatement::new());
        assert!(select.is_read_only());
        assert!(!select.is_ddl());

        let create = Statement::CreateTable(CreateTableStatement {
            name: TableRef::new("test"),
            columns: vec![],
            constraints: vec![],
            if_not_exists: false,
        });
        assert!(create.is_ddl());
        assert!(!create.is_read_only());

        assert!(Statement::Begin.is_txn_control());
        assert!(Statement::Commit.is_txn_control());
    }

    #[test]
    fn test_join() {
        let sql = "SELECT u.id, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id";
        let stmt = Parser::parse_one(sql).unwrap();

        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.from.len(), 1);
                match &select.from[0] {
                    FromItem::Join { join_type, on, .. } => {
                        assert_eq!(*join_type, JoinType::Left);
                        assert!(on.is_some());
                    }
                    _ => panic!("Expected JOIN"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }
}
