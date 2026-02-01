//! Optimization rules for the query optimizer.
//!
//! This module contains the individual optimization rules that transform
//! logical plans into more efficient forms.

use std::collections::HashSet;
use std::sync::Arc;

use crate::logical::{
    conjoin, extract_conjuncts, AggregateOperator, FilterOperator, JoinOperator, JoinType,
    LimitOperator, LogicalExpr, LogicalOperator, LogicalPlan, ProjectionOperator, ScanOperator,
    SortOperator,
};
use crate::parser::Literal;

use super::{OptimizerResult, OptimizerRule};

// ============================================================================
// Predicate Pushdown Rule
// ============================================================================

/// Pushes filter predicates closer to data sources.
///
/// This reduces the amount of data processed by moving filters past
/// projections, joins, and other operators when safe to do so.
///
/// # Example
///
/// Before:
/// ```text
/// Filter(age > 18)
///   Join
///     Scan(users)
///     Scan(orders)
/// ```
///
/// After:
/// ```text
/// Join
///   Filter(age > 18)
///     Scan(users)
///   Scan(orders)
/// ```
#[derive(Debug)]
pub struct PredicatePushdownRule;

impl OptimizerRule for PredicatePushdownRule {
    fn name(&self) -> &str {
        "PredicatePushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> OptimizerResult<Option<LogicalPlan>> {
        let new_root = self.push_predicates(&plan.root)?;
        Ok(new_root.map(|op| LogicalPlan::new(op)))
    }
}

impl PredicatePushdownRule {
    fn push_predicates(&self, op: &LogicalOperator) -> OptimizerResult<Option<LogicalOperator>> {
        match op {
            LogicalOperator::Filter(filter) => {
                // First, recursively optimize the input
                let optimized_input = self
                    .push_predicates(&filter.input)?
                    .map(Arc::new)
                    .unwrap_or_else(|| filter.input.clone());

                // Try to push the filter down
                self.try_push_filter(&filter.predicate, &optimized_input)
            }
            // Recursively process other operators
            LogicalOperator::Projection(proj) => {
                let new_input = self.push_predicates(&proj.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Projection(ProjectionOperator {
                        input: Arc::new(input),
                        exprs: proj.exprs.clone(),
                        schema: proj.schema.clone(),
                    })
                }))
            }
            LogicalOperator::Join(join) => {
                let left_opt = self.push_predicates(&join.left)?;
                let right_opt = self.push_predicates(&join.right)?;

                if left_opt.is_none() && right_opt.is_none() {
                    return Ok(None);
                }

                Ok(Some(LogicalOperator::Join(JoinOperator {
                    left: left_opt.map(Arc::new).unwrap_or_else(|| join.left.clone()),
                    right: right_opt
                        .map(Arc::new)
                        .unwrap_or_else(|| join.right.clone()),
                    join_type: join.join_type,
                    condition: join.condition.clone(),
                    schema: join.schema.clone(),
                    equi_keys: join.equi_keys.clone(),
                    filter: join.filter.clone(),
                })))
            }
            LogicalOperator::Aggregate(agg) => {
                let new_input = self.push_predicates(&agg.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Aggregate(AggregateOperator {
                        input: Arc::new(input),
                        group_by: agg.group_by.clone(),
                        aggregates: agg.aggregates.clone(),
                        schema: agg.schema.clone(),
                    })
                }))
            }
            LogicalOperator::Sort(sort) => {
                let new_input = self.push_predicates(&sort.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Sort(SortOperator {
                        input: Arc::new(input),
                        order_by: sort.order_by.clone(),
                        fetch: sort.fetch,
                    })
                }))
            }
            LogicalOperator::Limit(limit) => {
                let new_input = self.push_predicates(&limit.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Limit(LimitOperator {
                        input: Arc::new(input),
                        offset: limit.offset,
                        fetch: limit.fetch,
                    })
                }))
            }
            // Leaf operators - no changes
            _ => Ok(None),
        }
    }

    fn try_push_filter(
        &self,
        predicate: &LogicalExpr,
        input: &Arc<LogicalOperator>,
    ) -> OptimizerResult<Option<LogicalOperator>> {
        match input.as_ref() {
            // Push through projection if predicate only references projected columns
            LogicalOperator::Projection(proj) => {
                let pred_cols = predicate.columns();
                let proj_cols: HashSet<_> = proj
                    .exprs
                    .iter()
                    .flat_map(|e| e.columns())
                    .map(|c| c.name.clone())
                    .collect();

                // Check if all predicate columns are available in the projection input
                let can_push = pred_cols.iter().all(|c| {
                    proj.input.schema().index_of(&c.name).is_some() || proj_cols.contains(&c.name)
                });

                if can_push {
                    // Push filter below projection
                    let new_filter = LogicalOperator::Filter(FilterOperator {
                        input: proj.input.clone(),
                        predicate: predicate.clone(),
                    });
                    Ok(Some(LogicalOperator::Projection(ProjectionOperator {
                        input: Arc::new(new_filter),
                        exprs: proj.exprs.clone(),
                        schema: proj.schema.clone(),
                    })))
                } else {
                    // Can't push, keep filter here
                    Ok(None)
                }
            }

            // Push through inner join
            LogicalOperator::Join(join) if join.join_type == JoinType::Inner => {
                let left_schema = join.left.schema();
                let right_schema = join.right.schema();

                let conjuncts = extract_conjuncts(predicate);
                let mut left_predicates = Vec::new();
                let mut right_predicates = Vec::new();
                let mut remaining = Vec::new();

                for conjunct in conjuncts {
                    let cols = conjunct.columns();
                    let on_left = cols
                        .iter()
                        .all(|c| left_schema.index_of_column(c).is_some());
                    let on_right = cols
                        .iter()
                        .all(|c| right_schema.index_of_column(c).is_some());

                    if on_left && !on_right {
                        left_predicates.push(conjunct.clone());
                    } else if on_right && !on_left {
                        right_predicates.push(conjunct.clone());
                    } else {
                        remaining.push(conjunct.clone());
                    }
                }

                // If we can push something, do it
                if !left_predicates.is_empty() || !right_predicates.is_empty() {
                    let new_left = if !left_predicates.is_empty() {
                        Arc::new(LogicalOperator::Filter(FilterOperator {
                            input: join.left.clone(),
                            predicate: conjoin(left_predicates).unwrap(),
                        }))
                    } else {
                        join.left.clone()
                    };

                    let new_right = if !right_predicates.is_empty() {
                        Arc::new(LogicalOperator::Filter(FilterOperator {
                            input: join.right.clone(),
                            predicate: conjoin(right_predicates).unwrap(),
                        }))
                    } else {
                        join.right.clone()
                    };

                    let new_join = LogicalOperator::Join(JoinOperator {
                        left: new_left,
                        right: new_right,
                        join_type: join.join_type,
                        condition: join.condition.clone(),
                        schema: join.schema.clone(),
                        equi_keys: join.equi_keys.clone(),
                        filter: join.filter.clone(),
                    });

                    // Add remaining predicates as filter on top
                    if !remaining.is_empty() {
                        Ok(Some(LogicalOperator::Filter(FilterOperator {
                            input: Arc::new(new_join),
                            predicate: conjoin(remaining).unwrap(),
                        })))
                    } else {
                        Ok(Some(new_join))
                    }
                } else {
                    Ok(None)
                }
            }

            // Push into scan operator's filter list
            LogicalOperator::Scan(scan) => {
                let mut new_scan = scan.clone();
                new_scan.filters.push(predicate.clone());
                Ok(Some(LogicalOperator::Scan(new_scan)))
            }

            _ => Ok(None),
        }
    }
}

// ============================================================================
// Projection Pushdown Rule
// ============================================================================

/// Eliminates unused columns early in the plan.
///
/// This reduces memory usage and I/O by only reading the columns
/// that are actually needed for the query result.
#[derive(Debug)]
pub struct ProjectionPushdownRule;

impl OptimizerRule for ProjectionPushdownRule {
    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> OptimizerResult<Option<LogicalPlan>> {
        // Collect required columns from the root
        let required = self.required_columns(&plan.root);
        let new_root = self.push_projection(&plan.root, &required)?;
        Ok(new_root.map(|op| LogicalPlan::new(op)))
    }
}

impl ProjectionPushdownRule {
    fn required_columns(&self, op: &LogicalOperator) -> HashSet<String> {
        match op {
            LogicalOperator::Projection(proj) => proj
                .exprs
                .iter()
                .flat_map(|e| e.columns())
                .map(|c| c.name.clone())
                .collect(),
            LogicalOperator::Filter(filter) => {
                let mut cols = self.required_columns(&filter.input);
                cols.extend(
                    filter
                        .predicate
                        .columns()
                        .into_iter()
                        .map(|c| c.name.clone()),
                );
                cols
            }
            LogicalOperator::Join(join) => {
                let mut cols = self.required_columns(&join.left);
                cols.extend(self.required_columns(&join.right));
                if let Some(cond) = &join.condition {
                    cols.extend(cond.columns().into_iter().map(|c| c.name.clone()));
                }
                cols
            }
            LogicalOperator::Aggregate(agg) => {
                let mut cols: HashSet<_> = agg
                    .group_by
                    .iter()
                    .flat_map(|e| e.columns())
                    .map(|c| c.name.clone())
                    .collect();
                cols.extend(
                    agg.aggregates
                        .iter()
                        .flat_map(|e| e.columns())
                        .map(|c| c.name.clone()),
                );
                cols
            }
            LogicalOperator::Sort(sort) => {
                let mut cols = self.required_columns(&sort.input);
                cols.extend(
                    sort.order_by
                        .iter()
                        .flat_map(|e| e.expr.columns())
                        .map(|c| c.name.clone()),
                );
                cols
            }
            LogicalOperator::Scan(scan) => scan
                .projected_schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect(),
            _ => HashSet::new(),
        }
    }

    fn push_projection(
        &self,
        op: &LogicalOperator,
        required: &HashSet<String>,
    ) -> OptimizerResult<Option<LogicalOperator>> {
        match op {
            LogicalOperator::Scan(scan) => {
                // Check if we can eliminate columns
                let current_cols: Vec<_> = scan
                    .projected_schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect();

                // Find indices of required columns
                let new_indices: Vec<usize> = current_cols
                    .iter()
                    .enumerate()
                    .filter(|(_, name)| required.contains(&name.to_string()))
                    .map(|(i, _)| i)
                    .collect();

                // If we can eliminate columns
                if new_indices.len() < current_cols.len() && !new_indices.is_empty() {
                    let new_scan = ScanOperator {
                        table_name: scan.table_name.clone(),
                        table_schema: scan.table_schema.clone(),
                        projection: Some(new_indices.clone()),
                        projected_schema: Arc::new(scan.table_schema.project(&new_indices)),
                        filters: scan.filters.clone(),
                        limit: scan.limit,
                        table_meta: scan.table_meta.clone(),
                    };
                    Ok(Some(LogicalOperator::Scan(new_scan)))
                } else {
                    Ok(None)
                }
            }

            LogicalOperator::Filter(filter) => {
                // Add columns needed by predicate
                let mut needed = required.clone();
                needed.extend(
                    filter
                        .predicate
                        .columns()
                        .into_iter()
                        .map(|c| c.name.clone()),
                );

                let new_input = self.push_projection(&filter.input, &needed)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Filter(FilterOperator {
                        input: Arc::new(input),
                        predicate: filter.predicate.clone(),
                    })
                }))
            }

            LogicalOperator::Projection(proj) => {
                // Collect columns needed by projection expressions
                let needed: HashSet<_> = proj
                    .exprs
                    .iter()
                    .flat_map(|e| e.columns())
                    .map(|c| c.name.clone())
                    .collect();

                let new_input = self.push_projection(&proj.input, &needed)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Projection(ProjectionOperator {
                        input: Arc::new(input),
                        exprs: proj.exprs.clone(),
                        schema: proj.schema.clone(),
                    })
                }))
            }

            LogicalOperator::Join(join) => {
                let left_schema = join.left.schema();
                let right_schema = join.right.schema();

                // Split required columns between left and right
                let mut left_required = HashSet::new();
                let mut right_required = HashSet::new();

                for col in required {
                    if left_schema.index_of(col).is_some() {
                        left_required.insert(col.clone());
                    }
                    if right_schema.index_of(col).is_some() {
                        right_required.insert(col.clone());
                    }
                }

                // Add join condition columns
                if let Some(cond) = &join.condition {
                    for col in cond.columns() {
                        if left_schema.index_of(&col.name).is_some() {
                            left_required.insert(col.name.clone());
                        }
                        if right_schema.index_of(&col.name).is_some() {
                            right_required.insert(col.name.clone());
                        }
                    }
                }

                let left_opt = self.push_projection(&join.left, &left_required)?;
                let right_opt = self.push_projection(&join.right, &right_required)?;

                if left_opt.is_none() && right_opt.is_none() {
                    return Ok(None);
                }

                Ok(Some(LogicalOperator::Join(JoinOperator {
                    left: left_opt.map(Arc::new).unwrap_or_else(|| join.left.clone()),
                    right: right_opt
                        .map(Arc::new)
                        .unwrap_or_else(|| join.right.clone()),
                    join_type: join.join_type,
                    condition: join.condition.clone(),
                    schema: join.schema.clone(),
                    equi_keys: join.equi_keys.clone(),
                    filter: join.filter.clone(),
                })))
            }

            _ => Ok(None),
        }
    }
}

// ============================================================================
// Constant Folding Rule
// ============================================================================

/// Evaluates constant expressions at planning time.
///
/// This eliminates runtime computation for expressions that can be
/// fully evaluated during query planning.
///
/// # Example
///
/// Before: `SELECT 1 + 2, a FROM t`
/// After: `SELECT 3, a FROM t`
#[derive(Debug)]
pub struct ConstantFoldingRule;

impl OptimizerRule for ConstantFoldingRule {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn optimize(&self, plan: &LogicalPlan) -> OptimizerResult<Option<LogicalPlan>> {
        let new_root = self.fold_constants(&plan.root)?;
        Ok(new_root.map(|op| LogicalPlan::new(op)))
    }
}

impl ConstantFoldingRule {
    fn fold_constants(&self, op: &LogicalOperator) -> OptimizerResult<Option<LogicalOperator>> {
        match op {
            LogicalOperator::Projection(proj) => {
                let mut changed = false;
                let new_exprs: Vec<_> = proj
                    .exprs
                    .iter()
                    .map(|e| {
                        let folded = self.fold_expr(e);
                        if folded != *e {
                            changed = true;
                        }
                        folded
                    })
                    .collect();

                let new_input = self.fold_constants(&proj.input)?;
                if !changed && new_input.is_none() {
                    return Ok(None);
                }

                Ok(Some(LogicalOperator::Projection(ProjectionOperator {
                    input: new_input
                        .map(Arc::new)
                        .unwrap_or_else(|| proj.input.clone()),
                    exprs: new_exprs,
                    schema: proj.schema.clone(),
                })))
            }

            LogicalOperator::Filter(filter) => {
                let new_pred = self.fold_expr(&filter.predicate);
                let new_input = self.fold_constants(&filter.input)?;

                // Check if predicate is always true
                if matches!(&new_pred, LogicalExpr::Literal(Literal::Boolean(true))) {
                    // Remove the filter entirely
                    return Ok(Some(new_input.unwrap_or_else(|| (*filter.input).clone())));
                }

                if new_pred == filter.predicate && new_input.is_none() {
                    return Ok(None);
                }

                Ok(Some(LogicalOperator::Filter(FilterOperator {
                    input: new_input
                        .map(Arc::new)
                        .unwrap_or_else(|| filter.input.clone()),
                    predicate: new_pred,
                })))
            }

            LogicalOperator::Join(join) => {
                let left_opt = self.fold_constants(&join.left)?;
                let right_opt = self.fold_constants(&join.right)?;
                let new_condition = join.condition.as_ref().map(|c| self.fold_expr(c));
                let new_filter = join.filter.as_ref().map(|f| self.fold_expr(f));

                let condition_changed = new_condition.as_ref() != join.condition.as_ref();
                let filter_changed = new_filter.as_ref() != join.filter.as_ref();

                if left_opt.is_none()
                    && right_opt.is_none()
                    && !condition_changed
                    && !filter_changed
                {
                    return Ok(None);
                }

                Ok(Some(LogicalOperator::Join(JoinOperator {
                    left: left_opt.map(Arc::new).unwrap_or_else(|| join.left.clone()),
                    right: right_opt
                        .map(Arc::new)
                        .unwrap_or_else(|| join.right.clone()),
                    join_type: join.join_type,
                    condition: new_condition.or_else(|| join.condition.clone()),
                    schema: join.schema.clone(),
                    equi_keys: join.equi_keys.clone(),
                    filter: new_filter.or_else(|| join.filter.clone()),
                })))
            }

            LogicalOperator::Sort(sort) => {
                let new_input = self.fold_constants(&sort.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Sort(SortOperator {
                        input: Arc::new(input),
                        order_by: sort.order_by.clone(),
                        fetch: sort.fetch,
                    })
                }))
            }

            LogicalOperator::Limit(limit) => {
                let new_input = self.fold_constants(&limit.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Limit(LimitOperator {
                        input: Arc::new(input),
                        offset: limit.offset,
                        fetch: limit.fetch,
                    })
                }))
            }

            LogicalOperator::Aggregate(agg) => {
                let new_input = self.fold_constants(&agg.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Aggregate(AggregateOperator {
                        input: Arc::new(input),
                        group_by: agg.group_by.clone(),
                        aggregates: agg.aggregates.clone(),
                        schema: agg.schema.clone(),
                    })
                }))
            }

            _ => Ok(None),
        }
    }

    fn fold_expr(&self, expr: &LogicalExpr) -> LogicalExpr {
        match expr {
            LogicalExpr::BinaryOp { left, op, right } => {
                let left_folded = self.fold_expr(left);
                let right_folded = self.fold_expr(right);

                // Try to fold if both sides are literals
                if let (LogicalExpr::Literal(l), LogicalExpr::Literal(r)) =
                    (&left_folded, &right_folded)
                {
                    if let Some(result) = self.fold_binary_literals(l, op, r) {
                        return LogicalExpr::Literal(result);
                    }
                }

                LogicalExpr::BinaryOp {
                    left: Box::new(left_folded),
                    op: *op,
                    right: Box::new(right_folded),
                }
            }

            LogicalExpr::UnaryOp { op, expr: inner } => {
                let folded = self.fold_expr(inner);
                if let LogicalExpr::Literal(lit) = &folded {
                    if let Some(result) = self.fold_unary_literal(op, lit) {
                        return LogicalExpr::Literal(result);
                    }
                }
                LogicalExpr::UnaryOp {
                    op: *op,
                    expr: Box::new(folded),
                }
            }

            LogicalExpr::Alias { expr: inner, name } => LogicalExpr::Alias {
                expr: Box::new(self.fold_expr(inner)),
                name: name.clone(),
            },

            LogicalExpr::Case {
                operand,
                when_then,
                else_result,
            } => {
                let folded_when_then: Vec<_> = when_then
                    .iter()
                    .map(|(w, t)| (self.fold_expr(w), self.fold_expr(t)))
                    .collect();

                LogicalExpr::Case {
                    operand: operand.as_ref().map(|o| Box::new(self.fold_expr(o))),
                    when_then: folded_when_then,
                    else_result: else_result.as_ref().map(|e| Box::new(self.fold_expr(e))),
                }
            }

            // Other expressions - no folding
            _ => expr.clone(),
        }
    }

    fn fold_binary_literals(
        &self,
        left: &Literal,
        op: &crate::logical::BinaryOp,
        right: &Literal,
    ) -> Option<Literal> {
        use crate::logical::BinaryOp;

        match (left, op, right) {
            // Integer arithmetic
            (Literal::Integer(l), BinaryOp::Plus, Literal::Integer(r)) => {
                Some(Literal::Integer(l + r))
            }
            (Literal::Integer(l), BinaryOp::Minus, Literal::Integer(r)) => {
                Some(Literal::Integer(l - r))
            }
            (Literal::Integer(l), BinaryOp::Multiply, Literal::Integer(r)) => {
                Some(Literal::Integer(l * r))
            }
            (Literal::Integer(l), BinaryOp::Divide, Literal::Integer(r)) if *r != 0 => {
                Some(Literal::Integer(l / r))
            }
            (Literal::Integer(l), BinaryOp::Modulo, Literal::Integer(r)) if *r != 0 => {
                Some(Literal::Integer(l % r))
            }

            // Integer comparisons
            (Literal::Integer(l), BinaryOp::Eq, Literal::Integer(r)) => {
                Some(Literal::Boolean(l == r))
            }
            (Literal::Integer(l), BinaryOp::NotEq, Literal::Integer(r)) => {
                Some(Literal::Boolean(l != r))
            }
            (Literal::Integer(l), BinaryOp::Lt, Literal::Integer(r)) => {
                Some(Literal::Boolean(l < r))
            }
            (Literal::Integer(l), BinaryOp::LtEq, Literal::Integer(r)) => {
                Some(Literal::Boolean(l <= r))
            }
            (Literal::Integer(l), BinaryOp::Gt, Literal::Integer(r)) => {
                Some(Literal::Boolean(l > r))
            }
            (Literal::Integer(l), BinaryOp::GtEq, Literal::Integer(r)) => {
                Some(Literal::Boolean(l >= r))
            }

            // Boolean operations
            (Literal::Boolean(l), BinaryOp::And, Literal::Boolean(r)) => {
                Some(Literal::Boolean(*l && *r))
            }
            (Literal::Boolean(l), BinaryOp::Or, Literal::Boolean(r)) => {
                Some(Literal::Boolean(*l || *r))
            }

            // String concatenation
            (Literal::String(l), BinaryOp::Concat, Literal::String(r)) => {
                Some(Literal::String(format!("{}{}", l, r)))
            }

            // String comparisons
            (Literal::String(l), BinaryOp::Eq, Literal::String(r)) => {
                Some(Literal::Boolean(l == r))
            }
            (Literal::String(l), BinaryOp::NotEq, Literal::String(r)) => {
                Some(Literal::Boolean(l != r))
            }

            _ => None,
        }
    }

    fn fold_unary_literal(&self, op: &crate::logical::UnaryOp, lit: &Literal) -> Option<Literal> {
        use crate::logical::UnaryOp;

        match (op, lit) {
            (UnaryOp::Not, Literal::Boolean(b)) => Some(Literal::Boolean(!b)),
            (UnaryOp::Minus, Literal::Integer(i)) => Some(Literal::Integer(-i)),
            (UnaryOp::Minus, Literal::Float(f)) => Some(Literal::Float(-f)),
            (UnaryOp::Plus, Literal::Integer(i)) => Some(Literal::Integer(*i)),
            (UnaryOp::Plus, Literal::Float(f)) => Some(Literal::Float(*f)),
            _ => None,
        }
    }
}

// ============================================================================
// Filter Simplification Rule
// ============================================================================

/// Simplifies boolean expressions and removes redundant conditions.
///
/// # Simplifications
///
/// - `true AND x` → `x`
/// - `false AND x` → `false`
/// - `true OR x` → `true`
/// - `false OR x` → `x`
/// - `NOT NOT x` → `x`
/// - `x = x` → `true` (for non-nullable columns)
#[derive(Debug)]
pub struct FilterSimplificationRule;

impl OptimizerRule for FilterSimplificationRule {
    fn name(&self) -> &str {
        "FilterSimplification"
    }

    fn optimize(&self, plan: &LogicalPlan) -> OptimizerResult<Option<LogicalPlan>> {
        let new_root = self.simplify(&plan.root)?;
        Ok(new_root.map(|op| LogicalPlan::new(op)))
    }
}

impl FilterSimplificationRule {
    fn simplify(&self, op: &LogicalOperator) -> OptimizerResult<Option<LogicalOperator>> {
        match op {
            LogicalOperator::Filter(filter) => {
                let simplified = self.simplify_expr(&filter.predicate);
                let new_input = self.simplify(&filter.input)?;

                // Check for always-true predicate
                if matches!(&simplified, LogicalExpr::Literal(Literal::Boolean(true))) {
                    return Ok(Some(new_input.unwrap_or_else(|| (*filter.input).clone())));
                }

                // Check for always-false predicate - return empty relation
                if matches!(&simplified, LogicalExpr::Literal(Literal::Boolean(false))) {
                    return Ok(Some(LogicalOperator::EmptyRelation(
                        crate::logical::EmptyRelationOperator {
                            produce_one_row: false,
                            schema: filter.input.schema(),
                        },
                    )));
                }

                if simplified == filter.predicate && new_input.is_none() {
                    return Ok(None);
                }

                Ok(Some(LogicalOperator::Filter(FilterOperator {
                    input: new_input
                        .map(Arc::new)
                        .unwrap_or_else(|| filter.input.clone()),
                    predicate: simplified,
                })))
            }

            LogicalOperator::Projection(proj) => {
                let new_input = self.simplify(&proj.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Projection(ProjectionOperator {
                        input: Arc::new(input),
                        exprs: proj.exprs.clone(),
                        schema: proj.schema.clone(),
                    })
                }))
            }

            LogicalOperator::Join(join) => {
                let left_opt = self.simplify(&join.left)?;
                let right_opt = self.simplify(&join.right)?;

                if left_opt.is_none() && right_opt.is_none() {
                    return Ok(None);
                }

                Ok(Some(LogicalOperator::Join(JoinOperator {
                    left: left_opt.map(Arc::new).unwrap_or_else(|| join.left.clone()),
                    right: right_opt
                        .map(Arc::new)
                        .unwrap_or_else(|| join.right.clone()),
                    join_type: join.join_type,
                    condition: join.condition.clone(),
                    schema: join.schema.clone(),
                    equi_keys: join.equi_keys.clone(),
                    filter: join.filter.clone(),
                })))
            }

            LogicalOperator::Aggregate(agg) => {
                let new_input = self.simplify(&agg.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Aggregate(AggregateOperator {
                        input: Arc::new(input),
                        group_by: agg.group_by.clone(),
                        aggregates: agg.aggregates.clone(),
                        schema: agg.schema.clone(),
                    })
                }))
            }

            LogicalOperator::Sort(sort) => {
                let new_input = self.simplify(&sort.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Sort(SortOperator {
                        input: Arc::new(input),
                        order_by: sort.order_by.clone(),
                        fetch: sort.fetch,
                    })
                }))
            }

            LogicalOperator::Limit(limit) => {
                let new_input = self.simplify(&limit.input)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Limit(LimitOperator {
                        input: Arc::new(input),
                        offset: limit.offset,
                        fetch: limit.fetch,
                    })
                }))
            }

            _ => Ok(None),
        }
    }

    fn simplify_expr(&self, expr: &LogicalExpr) -> LogicalExpr {
        match expr {
            LogicalExpr::BinaryOp { left, op, right } => {
                use crate::logical::BinaryOp;

                let left_simplified = self.simplify_expr(left);
                let right_simplified = self.simplify_expr(right);

                match op {
                    // AND simplifications
                    BinaryOp::And => {
                        // true AND x → x
                        if matches!(
                            &left_simplified,
                            LogicalExpr::Literal(Literal::Boolean(true))
                        ) {
                            return right_simplified;
                        }
                        // x AND true → x
                        if matches!(
                            &right_simplified,
                            LogicalExpr::Literal(Literal::Boolean(true))
                        ) {
                            return left_simplified;
                        }
                        // false AND x → false
                        if matches!(
                            &left_simplified,
                            LogicalExpr::Literal(Literal::Boolean(false))
                        ) {
                            return LogicalExpr::Literal(Literal::Boolean(false));
                        }
                        // x AND false → false
                        if matches!(
                            &right_simplified,
                            LogicalExpr::Literal(Literal::Boolean(false))
                        ) {
                            return LogicalExpr::Literal(Literal::Boolean(false));
                        }
                    }

                    // OR simplifications
                    BinaryOp::Or => {
                        // true OR x → true
                        if matches!(
                            &left_simplified,
                            LogicalExpr::Literal(Literal::Boolean(true))
                        ) {
                            return LogicalExpr::Literal(Literal::Boolean(true));
                        }
                        // x OR true → true
                        if matches!(
                            &right_simplified,
                            LogicalExpr::Literal(Literal::Boolean(true))
                        ) {
                            return LogicalExpr::Literal(Literal::Boolean(true));
                        }
                        // false OR x → x
                        if matches!(
                            &left_simplified,
                            LogicalExpr::Literal(Literal::Boolean(false))
                        ) {
                            return right_simplified;
                        }
                        // x OR false → x
                        if matches!(
                            &right_simplified,
                            LogicalExpr::Literal(Literal::Boolean(false))
                        ) {
                            return left_simplified;
                        }
                    }

                    _ => {}
                }

                LogicalExpr::BinaryOp {
                    left: Box::new(left_simplified),
                    op: *op,
                    right: Box::new(right_simplified),
                }
            }

            LogicalExpr::UnaryOp { op, expr: inner } => {
                use crate::logical::UnaryOp;

                let simplified = self.simplify_expr(inner);

                // NOT NOT x → x
                if matches!(op, UnaryOp::Not) {
                    if let LogicalExpr::UnaryOp {
                        op: UnaryOp::Not,
                        expr: inner_inner,
                    } = &simplified
                    {
                        return (**inner_inner).clone();
                    }
                }

                LogicalExpr::UnaryOp {
                    op: *op,
                    expr: Box::new(simplified),
                }
            }

            _ => expr.clone(),
        }
    }
}

// ============================================================================
// Limit Pushdown Rule
// ============================================================================

/// Pushes LIMIT through compatible operators.
///
/// This can reduce intermediate result sizes when LIMIT can be
/// applied earlier in the plan.
#[derive(Debug)]
pub struct LimitPushdownRule;

impl OptimizerRule for LimitPushdownRule {
    fn name(&self) -> &str {
        "LimitPushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> OptimizerResult<Option<LogicalPlan>> {
        let new_root = self.push_limit(&plan.root, None)?;
        Ok(new_root.map(|op| LogicalPlan::new(op)))
    }
}

impl LimitPushdownRule {
    fn push_limit(
        &self,
        op: &LogicalOperator,
        limit: Option<usize>,
    ) -> OptimizerResult<Option<LogicalOperator>> {
        match op {
            LogicalOperator::Limit(limit_op) => {
                // Combine limits - take the smaller one
                let effective_limit = match (limit, limit_op.fetch) {
                    (Some(outer), Some(inner)) => Some(outer.min(inner)),
                    (Some(l), None) | (None, Some(l)) => Some(l),
                    (None, None) => None,
                };

                let new_input = self.push_limit(&limit_op.input, effective_limit)?;

                if new_input.is_some() || effective_limit != limit_op.fetch {
                    Ok(Some(LogicalOperator::Limit(LimitOperator {
                        input: new_input
                            .map(Arc::new)
                            .unwrap_or_else(|| limit_op.input.clone()),
                        offset: limit_op.offset,
                        fetch: effective_limit,
                    })))
                } else {
                    Ok(None)
                }
            }

            // Push limit into sort to enable TopN optimization
            LogicalOperator::Sort(sort) if limit.is_some() => {
                let new_input = self.push_limit(&sort.input, None)?;

                if sort.fetch.is_none() {
                    Ok(Some(LogicalOperator::Sort(SortOperator {
                        input: new_input
                            .map(Arc::new)
                            .unwrap_or_else(|| sort.input.clone()),
                        order_by: sort.order_by.clone(),
                        fetch: limit,
                    })))
                } else {
                    let new_fetch = match (sort.fetch, limit) {
                        (Some(s), Some(l)) => Some(s.min(l)),
                        (Some(s), None) => Some(s),
                        (None, Some(l)) => Some(l),
                        (None, None) => None,
                    };
                    Ok(Some(LogicalOperator::Sort(SortOperator {
                        input: new_input
                            .map(Arc::new)
                            .unwrap_or_else(|| sort.input.clone()),
                        order_by: sort.order_by.clone(),
                        fetch: new_fetch,
                    })))
                }
            }

            // Push limit into scan
            LogicalOperator::Scan(scan) if limit.is_some() && scan.limit.is_none() => {
                let mut new_scan = scan.clone();
                new_scan.limit = limit;
                Ok(Some(LogicalOperator::Scan(new_scan)))
            }

            // Recursively process other operators
            LogicalOperator::Projection(proj) => {
                let new_input = self.push_limit(&proj.input, None)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Projection(ProjectionOperator {
                        input: Arc::new(input),
                        exprs: proj.exprs.clone(),
                        schema: proj.schema.clone(),
                    })
                }))
            }

            LogicalOperator::Filter(filter) => {
                let new_input = self.push_limit(&filter.input, None)?;
                Ok(new_input.map(|input| {
                    LogicalOperator::Filter(FilterOperator {
                        input: Arc::new(input),
                        predicate: filter.predicate.clone(),
                    })
                }))
            }

            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::{Field, Schema};
    use crate::parser::DataType;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
            Field::nullable("age", DataType::Int),
        ])
    }

    #[test]
    fn test_constant_folding_integers() {
        let rule = ConstantFoldingRule;

        let scan = ScanOperator::new("users", test_schema());
        let proj = ProjectionOperator {
            input: Arc::new(LogicalOperator::Scan(scan)),
            exprs: vec![LogicalExpr::lit_i64(1).add(LogicalExpr::lit_i64(2))],
            schema: Arc::new(Schema::new(vec![Field::not_null("result", DataType::Int)])),
        };
        let plan = LogicalPlan::new(LogicalOperator::Projection(proj));

        let result = rule.optimize(&plan).unwrap();
        assert!(result.is_some());
        let optimized = result.unwrap();

        if let LogicalOperator::Projection(p) = optimized.root.as_ref() {
            assert!(matches!(
                &p.exprs[0],
                LogicalExpr::Literal(Literal::Integer(3))
            ));
        } else {
            panic!("Expected projection");
        }
    }

    #[test]
    fn test_filter_simplification_and_true() {
        let rule = FilterSimplificationRule;

        let scan = ScanOperator::new("users", test_schema());
        let filter = FilterOperator {
            input: Arc::new(LogicalOperator::Scan(scan)),
            predicate: LogicalExpr::col("id")
                .eq(LogicalExpr::lit_i64(1))
                .and(LogicalExpr::lit_bool(true)),
        };
        let plan = LogicalPlan::new(LogicalOperator::Filter(filter));

        let result = rule.optimize(&plan).unwrap();
        assert!(result.is_some());
        let optimized = result.unwrap();

        if let LogicalOperator::Filter(f) = optimized.root.as_ref() {
            // Should simplify to just id = 1
            assert!(matches!(
                &f.predicate,
                LogicalExpr::BinaryOp {
                    op: crate::logical::BinaryOp::Eq,
                    ..
                }
            ));
        } else {
            panic!("Expected filter");
        }
    }

    #[test]
    fn test_filter_always_true_removed() {
        let rule = FilterSimplificationRule;

        let scan = ScanOperator::new("users", test_schema());
        let filter = FilterOperator {
            input: Arc::new(LogicalOperator::Scan(scan)),
            predicate: LogicalExpr::lit_bool(true),
        };
        let plan = LogicalPlan::new(LogicalOperator::Filter(filter));

        let result = rule.optimize(&plan).unwrap();
        assert!(result.is_some());
        let optimized = result.unwrap();

        // Filter should be removed, leaving just the scan
        assert!(matches!(optimized.root.as_ref(), LogicalOperator::Scan(_)));
    }

    #[test]
    fn test_filter_always_false_returns_empty() {
        let rule = FilterSimplificationRule;

        let scan = ScanOperator::new("users", test_schema());
        let filter = FilterOperator {
            input: Arc::new(LogicalOperator::Scan(scan)),
            predicate: LogicalExpr::lit_bool(false),
        };
        let plan = LogicalPlan::new(LogicalOperator::Filter(filter));

        let result = rule.optimize(&plan).unwrap();
        assert!(result.is_some());
        let optimized = result.unwrap();

        // Should return empty relation
        assert!(matches!(
            optimized.root.as_ref(),
            LogicalOperator::EmptyRelation(_)
        ));
    }

    #[test]
    fn test_limit_pushdown_to_sort() {
        let rule = LimitPushdownRule;

        let scan = ScanOperator::new("users", test_schema());
        let sort = SortOperator {
            input: Arc::new(LogicalOperator::Scan(scan)),
            order_by: vec![crate::logical::SortExpr {
                expr: LogicalExpr::col("id"),
                asc: true,
                nulls_first: false,
            }],
            fetch: None,
        };
        let limit = LimitOperator {
            input: Arc::new(LogicalOperator::Sort(sort)),
            offset: 0,
            fetch: Some(10),
        };
        let plan = LogicalPlan::new(LogicalOperator::Limit(limit));

        let result = rule.optimize(&plan).unwrap();
        assert!(result.is_some());
        let optimized = result.unwrap();

        // Limit should be pushed into sort
        if let LogicalOperator::Limit(l) = optimized.root.as_ref() {
            if let LogicalOperator::Sort(s) = l.input.as_ref() {
                assert_eq!(s.fetch, Some(10));
            } else {
                panic!("Expected sort");
            }
        } else {
            panic!("Expected limit");
        }
    }

    #[test]
    fn test_predicate_pushdown_through_projection() {
        let rule = PredicatePushdownRule;

        let scan = ScanOperator::new("users", test_schema());
        let proj = ProjectionOperator {
            input: Arc::new(LogicalOperator::Scan(scan)),
            exprs: vec![LogicalExpr::col("id"), LogicalExpr::col("name")],
            schema: Arc::new(Schema::new(vec![
                Field::not_null("id", DataType::Int),
                Field::nullable("name", DataType::Varchar(Some(255))),
            ])),
        };
        let filter = FilterOperator {
            input: Arc::new(LogicalOperator::Projection(proj)),
            predicate: LogicalExpr::col("id").eq(LogicalExpr::lit_i64(1)),
        };
        let plan = LogicalPlan::new(LogicalOperator::Filter(filter));

        let result = rule.optimize(&plan).unwrap();
        assert!(result.is_some());
        let optimized = result.unwrap();

        // Filter should be pushed below projection
        if let LogicalOperator::Projection(p) = optimized.root.as_ref() {
            assert!(matches!(p.input.as_ref(), LogicalOperator::Filter(_)));
        } else {
            panic!("Expected projection at top");
        }
    }
}
