//! Logical plan wrapper and utilities.

use std::fmt;
use std::sync::Arc;

use super::operator::LogicalOperator;
use super::schema::SchemaRef;

/// A logical query plan.
#[derive(Debug, Clone)]
pub struct LogicalPlan {
    /// Root operator.
    pub root: Arc<LogicalOperator>,
}

impl LogicalPlan {
    /// Creates a new logical plan.
    pub fn new(root: LogicalOperator) -> Self {
        Self {
            root: Arc::new(root),
        }
    }

    /// Creates from an Arc.
    pub fn from_arc(root: Arc<LogicalOperator>) -> Self {
        Self { root }
    }

    /// Returns the output schema.
    pub fn schema(&self) -> SchemaRef {
        self.root.schema()
    }

    /// Returns a formatted string representation of the plan.
    pub fn display(&self) -> String {
        let mut output = String::new();
        self.format_node(&self.root, 0, &mut output);
        output
    }

    /// Returns a detailed formatted string for EXPLAIN.
    pub fn explain(&self) -> String {
        let mut output = String::new();
        self.explain_node(&self.root, 0, &mut output);
        output
    }

    fn format_node(&self, op: &LogicalOperator, indent: usize, output: &mut String) {
        let prefix = "  ".repeat(indent);

        match op {
            LogicalOperator::Scan(scan) => {
                output.push_str(&format!("{}Scan: {}", prefix, scan.table_name));
                if !scan.filters.is_empty() {
                    output.push_str(&format!(" (filters: {})", scan.filters.len()));
                }
                if let Some(proj) = &scan.projection {
                    output.push_str(&format!(" (projection: {:?})", proj));
                }
                output.push('\n');
            }
            LogicalOperator::Projection(proj) => {
                let exprs: Vec<_> = proj.exprs.iter().map(|e| e.output_name()).collect();
                output.push_str(&format!("{}Projection: {}\n", prefix, exprs.join(", ")));
                self.format_node(&proj.input, indent + 1, output);
            }
            LogicalOperator::Filter(filter) => {
                output.push_str(&format!(
                    "{}Filter: {}\n",
                    prefix,
                    filter.predicate.output_name()
                ));
                self.format_node(&filter.input, indent + 1, output);
            }
            LogicalOperator::Join(join) => {
                output.push_str(&format!("{}Join: {} JOIN", prefix, join.join_type));
                if !join.equi_keys.is_empty() {
                    let keys: Vec<_> = join
                        .equi_keys
                        .iter()
                        .map(|(l, r)| format!("{} = {}", l.output_name(), r.output_name()))
                        .collect();
                    output.push_str(&format!(" ON {}", keys.join(" AND ")));
                }
                output.push('\n');
                self.format_node(&join.left, indent + 1, output);
                self.format_node(&join.right, indent + 1, output);
            }
            LogicalOperator::Aggregate(agg) => {
                let groups: Vec<_> = agg.group_by.iter().map(|e| e.output_name()).collect();
                let aggs: Vec<_> = agg.aggregates.iter().map(|e| e.output_name()).collect();
                output.push_str(&format!(
                    "{}Aggregate: groups=[{}] aggs=[{}]\n",
                    prefix,
                    groups.join(", "),
                    aggs.join(", ")
                ));
                self.format_node(&agg.input, indent + 1, output);
            }
            LogicalOperator::Sort(sort) => {
                let exprs: Vec<_> = sort.order_by.iter().map(|e| e.to_string()).collect();
                output.push_str(&format!("{}Sort: {}\n", prefix, exprs.join(", ")));
                self.format_node(&sort.input, indent + 1, output);
            }
            LogicalOperator::Limit(limit) => {
                output.push_str(&format!(
                    "{}Limit: offset={} fetch={:?}\n",
                    prefix, limit.offset, limit.fetch
                ));
                self.format_node(&limit.input, indent + 1, output);
            }
            LogicalOperator::Distinct(distinct) => {
                output.push_str(&format!("{}Distinct\n", prefix));
                self.format_node(&distinct.input, indent + 1, output);
            }
            LogicalOperator::SetOperation(setop) => {
                output.push_str(&format!("{}SetOp: {}\n", prefix, setop.op));
                self.format_node(&setop.left, indent + 1, output);
                self.format_node(&setop.right, indent + 1, output);
            }
            LogicalOperator::SubqueryAlias(alias) => {
                output.push_str(&format!("{}SubqueryAlias: {}\n", prefix, alias.alias));
                self.format_node(&alias.input, indent + 1, output);
            }
            LogicalOperator::Values(values) => {
                output.push_str(&format!("{}Values: {} rows\n", prefix, values.values.len()));
            }
            LogicalOperator::EmptyRelation(empty) => {
                output.push_str(&format!(
                    "{}EmptyRelation: produce_one={}\n",
                    prefix, empty.produce_one_row
                ));
            }
            LogicalOperator::Window(window) => {
                output.push_str(&format!(
                    "{}Window: {} exprs\n",
                    prefix,
                    window.window_exprs.len()
                ));
                self.format_node(&window.input, indent + 1, output);
            }
            LogicalOperator::Cte(cte) => {
                output.push_str(&format!("{}CTE: {:?}\n", prefix, cte.cte_names));
                self.format_node(&cte.input, indent + 1, output);
            }
        }
    }

    fn explain_node(&self, op: &LogicalOperator, indent: usize, output: &mut String) {
        let prefix = "  ".repeat(indent);

        match op {
            LogicalOperator::Scan(scan) => {
                output.push_str(&format!("{}Scan: {}\n", prefix, scan.table_name));
                output.push_str(&format!("{}  Schema: {}\n", prefix, scan.projected_schema));
                if !scan.filters.is_empty() {
                    output.push_str(&format!("{}  Filters:\n", prefix));
                    for filter in &scan.filters {
                        output.push_str(&format!("{}    - {}\n", prefix, filter.output_name()));
                    }
                }
                if let Some(proj) = &scan.projection {
                    output.push_str(&format!("{}  Projection: {:?}\n", prefix, proj));
                }
                if let Some(limit) = scan.limit {
                    output.push_str(&format!("{}  Limit: {}\n", prefix, limit));
                }
            }
            LogicalOperator::Projection(proj) => {
                output.push_str(&format!("{}Projection:\n", prefix));
                output.push_str(&format!("{}  Schema: {}\n", prefix, proj.schema));
                output.push_str(&format!("{}  Expressions:\n", prefix));
                for expr in &proj.exprs {
                    output.push_str(&format!("{}    - {}\n", prefix, expr.output_name()));
                }
                self.explain_node(&proj.input, indent + 1, output);
            }
            LogicalOperator::Filter(filter) => {
                output.push_str(&format!("{}Filter:\n", prefix));
                output.push_str(&format!(
                    "{}  Predicate: {}\n",
                    prefix,
                    filter.predicate.output_name()
                ));
                self.explain_node(&filter.input, indent + 1, output);
            }
            LogicalOperator::Join(join) => {
                output.push_str(&format!("{}Join: {} JOIN\n", prefix, join.join_type));
                output.push_str(&format!("{}  Schema: {}\n", prefix, join.schema));
                if !join.equi_keys.is_empty() {
                    output.push_str(&format!("{}  Equi-keys:\n", prefix));
                    for (l, r) in &join.equi_keys {
                        output.push_str(&format!(
                            "{}    {} = {}\n",
                            prefix,
                            l.output_name(),
                            r.output_name()
                        ));
                    }
                }
                if let Some(filter) = &join.filter {
                    output.push_str(&format!("{}  Filter: {}\n", prefix, filter.output_name()));
                }
                self.explain_node(&join.left, indent + 1, output);
                self.explain_node(&join.right, indent + 1, output);
            }
            LogicalOperator::Aggregate(agg) => {
                output.push_str(&format!("{}Aggregate:\n", prefix));
                output.push_str(&format!("{}  Schema: {}\n", prefix, agg.schema));
                if !agg.group_by.is_empty() {
                    output.push_str(&format!("{}  Group By:\n", prefix));
                    for expr in &agg.group_by {
                        output.push_str(&format!("{}    - {}\n", prefix, expr.output_name()));
                    }
                }
                output.push_str(&format!("{}  Aggregates:\n", prefix));
                for expr in &agg.aggregates {
                    output.push_str(&format!("{}    - {}\n", prefix, expr.output_name()));
                }
                self.explain_node(&agg.input, indent + 1, output);
            }
            LogicalOperator::Sort(sort) => {
                output.push_str(&format!("{}Sort:\n", prefix));
                for expr in &sort.order_by {
                    output.push_str(&format!("{}  - {}\n", prefix, expr));
                }
                if let Some(fetch) = sort.fetch {
                    output.push_str(&format!("{}  Fetch: {}\n", prefix, fetch));
                }
                self.explain_node(&sort.input, indent + 1, output);
            }
            LogicalOperator::Limit(limit) => {
                output.push_str(&format!(
                    "{}Limit: offset={} fetch={:?}\n",
                    prefix, limit.offset, limit.fetch
                ));
                self.explain_node(&limit.input, indent + 1, output);
            }
            _ => {
                output.push_str(&format!("{}{}\n", prefix, op.name()));
                for child in op.children() {
                    self.explain_node(child, indent + 1, output);
                }
            }
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

/// Visitor trait for traversing logical plans.
pub trait PlanVisitor {
    /// Called before visiting children.
    fn pre_visit(&mut self, op: &LogicalOperator) -> bool;
    /// Called after visiting children.
    fn post_visit(&mut self, op: &LogicalOperator);
}

/// Visit a logical plan tree.
pub fn visit_plan<V: PlanVisitor>(op: &LogicalOperator, visitor: &mut V) {
    if visitor.pre_visit(op) {
        for child in op.children() {
            visit_plan(child, visitor);
        }
    }
    visitor.post_visit(op);
}

/// Rewriter trait for transforming logical plans.
pub trait PlanRewriter {
    /// Rewrite an operator. Return None to keep the original.
    fn rewrite(&mut self, op: LogicalOperator) -> Option<LogicalOperator>;
}

/// Rewrite a logical plan tree bottom-up.
pub fn rewrite_plan<R: PlanRewriter>(
    op: Arc<LogicalOperator>,
    rewriter: &mut R,
) -> Arc<LogicalOperator> {
    // First, rewrite children
    let op_with_new_children = rewrite_children(op, rewriter);

    // Then, rewrite this node
    match Arc::try_unwrap(op_with_new_children) {
        Ok(owned) => match rewriter.rewrite(owned) {
            Some(new_op) => Arc::new(new_op),
            None => panic!("Rewriter must return operator"),
        },
        Err(arc) => {
            // If we can't unwrap, clone
            match rewriter.rewrite((*arc).clone()) {
                Some(new_op) => Arc::new(new_op),
                None => arc,
            }
        }
    }
}

fn rewrite_children<R: PlanRewriter>(
    op: Arc<LogicalOperator>,
    rewriter: &mut R,
) -> Arc<LogicalOperator> {
    match &*op {
        LogicalOperator::Scan(_)
        | LogicalOperator::Values(_)
        | LogicalOperator::EmptyRelation(_) => op,

        LogicalOperator::Projection(proj) => {
            let new_input = rewrite_plan(proj.input.clone(), rewriter);
            if Arc::ptr_eq(&new_input, &proj.input) {
                op
            } else {
                Arc::new(LogicalOperator::Projection(
                    super::operator::ProjectionOperator {
                        input: new_input,
                        exprs: proj.exprs.clone(),
                        schema: proj.schema.clone(),
                    },
                ))
            }
        }
        LogicalOperator::Filter(filter) => {
            let new_input = rewrite_plan(filter.input.clone(), rewriter);
            if Arc::ptr_eq(&new_input, &filter.input) {
                op
            } else {
                Arc::new(LogicalOperator::Filter(super::operator::FilterOperator {
                    input: new_input,
                    predicate: filter.predicate.clone(),
                }))
            }
        }
        LogicalOperator::Join(join) => {
            let new_left = rewrite_plan(join.left.clone(), rewriter);
            let new_right = rewrite_plan(join.right.clone(), rewriter);
            if Arc::ptr_eq(&new_left, &join.left) && Arc::ptr_eq(&new_right, &join.right) {
                op
            } else {
                Arc::new(LogicalOperator::Join(super::operator::JoinOperator {
                    left: new_left,
                    right: new_right,
                    join_type: join.join_type,
                    condition: join.condition.clone(),
                    schema: join.schema.clone(),
                    equi_keys: join.equi_keys.clone(),
                    filter: join.filter.clone(),
                }))
            }
        }
        LogicalOperator::Aggregate(agg) => {
            let new_input = rewrite_plan(agg.input.clone(), rewriter);
            if Arc::ptr_eq(&new_input, &agg.input) {
                op
            } else {
                Arc::new(LogicalOperator::Aggregate(
                    super::operator::AggregateOperator {
                        input: new_input,
                        group_by: agg.group_by.clone(),
                        aggregates: agg.aggregates.clone(),
                        schema: agg.schema.clone(),
                    },
                ))
            }
        }
        LogicalOperator::Sort(sort) => {
            let new_input = rewrite_plan(sort.input.clone(), rewriter);
            if Arc::ptr_eq(&new_input, &sort.input) {
                op
            } else {
                Arc::new(LogicalOperator::Sort(super::operator::SortOperator {
                    input: new_input,
                    order_by: sort.order_by.clone(),
                    fetch: sort.fetch,
                }))
            }
        }
        LogicalOperator::Limit(limit) => {
            let new_input = rewrite_plan(limit.input.clone(), rewriter);
            if Arc::ptr_eq(&new_input, &limit.input) {
                op
            } else {
                Arc::new(LogicalOperator::Limit(super::operator::LimitOperator {
                    input: new_input,
                    offset: limit.offset,
                    fetch: limit.fetch,
                }))
            }
        }
        LogicalOperator::Distinct(distinct) => {
            let new_input = rewrite_plan(distinct.input.clone(), rewriter);
            if Arc::ptr_eq(&new_input, &distinct.input) {
                op
            } else {
                Arc::new(LogicalOperator::Distinct(
                    super::operator::DistinctOperator {
                        input: new_input,
                        on_columns: distinct.on_columns.clone(),
                    },
                ))
            }
        }
        LogicalOperator::SetOperation(setop) => {
            let new_left = rewrite_plan(setop.left.clone(), rewriter);
            let new_right = rewrite_plan(setop.right.clone(), rewriter);
            if Arc::ptr_eq(&new_left, &setop.left) && Arc::ptr_eq(&new_right, &setop.right) {
                op
            } else {
                Arc::new(LogicalOperator::SetOperation(
                    super::operator::SetOperationOperator {
                        left: new_left,
                        right: new_right,
                        op: setop.op,
                    },
                ))
            }
        }
        LogicalOperator::SubqueryAlias(alias) => {
            let new_input = rewrite_plan(alias.input.clone(), rewriter);
            if Arc::ptr_eq(&new_input, &alias.input) {
                op
            } else {
                Arc::new(LogicalOperator::SubqueryAlias(
                    super::operator::SubqueryAliasOperator {
                        input: new_input,
                        alias: alias.alias.clone(),
                        schema: alias.schema.clone(),
                    },
                ))
            }
        }
        LogicalOperator::Window(window) => {
            let new_input = rewrite_plan(window.input.clone(), rewriter);
            if Arc::ptr_eq(&new_input, &window.input) {
                op
            } else {
                Arc::new(LogicalOperator::Window(super::operator::WindowOperator {
                    input: new_input,
                    window_exprs: window.window_exprs.clone(),
                    schema: window.schema.clone(),
                }))
            }
        }
        LogicalOperator::Cte(cte) => {
            let new_input = rewrite_plan(cte.input.clone(), rewriter);
            let new_cte_plans: Vec<_> = cte
                .cte_plans
                .iter()
                .map(|p| rewrite_plan(p.clone(), rewriter))
                .collect();
            Arc::new(LogicalOperator::Cte(super::operator::CteOperator {
                input: new_input,
                cte_plans: new_cte_plans,
                cte_names: cte.cte_names.clone(),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::operator::ScanOperator;
    use crate::logical::schema::{Field, Schema};
    use crate::parser::DataType;

    #[test]
    fn test_plan_display() {
        let schema = Schema::new(vec![
            Field::not_null("id", DataType::Int),
            Field::nullable("name", DataType::Varchar(Some(255))),
        ]);
        let scan = ScanOperator::new("users", schema);
        let plan = LogicalPlan::new(LogicalOperator::Scan(scan));

        let display = plan.display();
        assert!(display.contains("Scan: users"));
    }

    #[test]
    fn test_plan_schema() {
        let schema = Schema::new(vec![Field::not_null("id", DataType::Int)]);
        let scan = ScanOperator::new("users", schema);
        let plan = LogicalPlan::new(LogicalOperator::Scan(scan));

        assert_eq!(plan.schema().len(), 1);
    }
}
