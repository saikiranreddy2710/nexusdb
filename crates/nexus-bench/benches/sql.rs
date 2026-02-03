//! SQL engine benchmarks for NexusDB.
//!
//! Benchmarks for:
//! - SQL parsing
//! - Query planning
//! - Query optimization
//! - Query execution

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nexus_server::database::Database;
use nexus_sql::optimizer::{Optimizer, OptimizerConfig};
use nexus_sql::parser::Parser;
use std::sync::Arc;

/// Simple SELECT queries of varying complexity.
fn simple_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("select_literal", "SELECT 1"),
        ("select_arithmetic", "SELECT 1 + 2 * 3 - 4 / 2"),
        ("select_star", "SELECT * FROM users"),
        ("select_columns", "SELECT id, name, age FROM users"),
        ("select_where_eq", "SELECT * FROM users WHERE id = 1"),
        (
            "select_where_and",
            "SELECT * FROM users WHERE id > 1 AND age < 50",
        ),
        ("select_order_by", "SELECT * FROM users ORDER BY name ASC"),
        ("select_limit", "SELECT * FROM users LIMIT 10"),
        (
            "select_complex",
            "SELECT id, name FROM users WHERE age > 25 ORDER BY name LIMIT 100",
        ),
    ]
}

/// Data manipulation queries.
fn dml_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("insert_simple", "INSERT INTO users VALUES (1, 'Alice', 30)"),
        (
            "insert_columns",
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
        ),
        ("update_simple", "UPDATE users SET age = 31 WHERE id = 1"),
        (
            "update_multiple",
            "UPDATE users SET name = 'Bob', age = 25 WHERE id = 2",
        ),
        ("delete_simple", "DELETE FROM users WHERE id = 1"),
        (
            "delete_condition",
            "DELETE FROM users WHERE age > 50 AND active = false",
        ),
    ]
}

/// DDL queries.
fn ddl_queries() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "create_table_simple",
            "CREATE TABLE test (id INT PRIMARY KEY)",
        ),
        (
            "create_table_multi",
            "CREATE TABLE test (id INT PRIMARY KEY, name TEXT, age INT, active BOOLEAN)",
        ),
        (
            "create_table_if_not_exists",
            "CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY)",
        ),
        ("drop_table", "DROP TABLE test"),
        ("drop_table_if_exists", "DROP TABLE IF EXISTS test"),
    ]
}

/// Benchmark SQL parsing.
fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql/parse");

    for (name, sql) in simple_queries()
        .into_iter()
        .chain(dml_queries().into_iter())
        .chain(ddl_queries().into_iter())
    {
        group.bench_with_input(BenchmarkId::new("query", name), &sql, |b, sql| {
            b.iter(|| {
                let result = Parser::parse(black_box(sql));
                black_box(result)
            });
        });
    }

    group.finish();
}

/// Benchmark parsing throughput (many queries).
fn bench_parse_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql/parse_throughput");

    let queries: Vec<_> = simple_queries().iter().map(|(_, q)| *q).collect();

    for count in [100, 500, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                let mut parsed = 0;
                for i in 0..count {
                    let sql = queries[i % queries.len()];
                    if Parser::parse(sql).is_ok() {
                        parsed += 1;
                    }
                }
                black_box(parsed)
            });
        });
    }

    group.finish();
}

/// Benchmark query optimization.
fn bench_optimize(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql/optimize");

    // We need a database to build logical plans
    let db = Database::open_memory().expect("Failed to create database");
    let db = Arc::new(db);

    // Create a test table
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
        .expect("Failed to create table");

    let optimizer = Optimizer::new(OptimizerConfig::default());

    // Test optimization of various query patterns
    let optimization_cases = vec![
        ("projection_pushdown", "SELECT id FROM users WHERE age > 25"),
        (
            "filter_combination",
            "SELECT * FROM users WHERE id > 1 AND id < 100",
        ),
        ("constant_folding", "SELECT * FROM users WHERE 1 = 1"),
    ];

    for (name, sql) in optimization_cases {
        group.bench_with_input(BenchmarkId::new("query", name), &sql, |b, sql| {
            // Parse and plan once
            let stmts = Parser::parse(sql).expect("Parse failed");
            let stmt = &stmts[0];

            // We'd need to build a logical plan here, but for now we just parse repeatedly
            b.iter(|| {
                let _ = Parser::parse(black_box(sql));
                // In a full implementation, we'd also run optimizer.optimize(plan)
            });
        });
    }

    group.finish();
}

/// Benchmark full query execution through the database.
fn bench_execute(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql/execute");

    let db = Database::open_memory().expect("Failed to create database");
    let db = Arc::new(db);

    // Create table and insert test data
    db.execute("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")
        .expect("Failed to create table");

    for i in 0..1000 {
        db.execute(&format!(
            "INSERT INTO bench VALUES ({}, 'item_{}', {})",
            i,
            i,
            i * 10
        ))
        .expect("Failed to insert");
    }

    // Benchmark SELECT queries
    group.bench_function("select_all", |b| {
        b.iter(|| {
            let result = db.execute("SELECT * FROM bench");
            black_box(result)
        });
    });

    group.bench_function("select_where", |b| {
        b.iter(|| {
            let result = db.execute("SELECT * FROM bench WHERE id = 500");
            black_box(result)
        });
    });

    group.bench_function("select_range", |b| {
        b.iter(|| {
            let result = db.execute("SELECT * FROM bench WHERE id > 400 AND id < 600");
            black_box(result)
        });
    });

    group.bench_function("select_limit", |b| {
        b.iter(|| {
            let result = db.execute("SELECT * FROM bench LIMIT 50");
            black_box(result)
        });
    });

    group.bench_function("select_order_limit", |b| {
        b.iter(|| {
            let result = db.execute("SELECT * FROM bench ORDER BY value LIMIT 50");
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark INSERT throughput.
fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql/insert");

    group.bench_function("single_insert", |b| {
        b.iter_with_setup(
            || {
                let db = Database::open_memory().expect("Failed to create database");
                db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
                    .expect("Failed to create table");
                (db, 0)
            },
            |(db, mut counter)| {
                counter += 1;
                let result =
                    db.execute(&format!("INSERT INTO t VALUES ({}, {})", counter, counter));
                black_box(result)
            },
        );
    });

    for count in [100, 500, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("batch", count), count, |b, &count| {
            b.iter(|| {
                let db = Database::open_memory().expect("Failed to create database");
                db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
                    .expect("Failed to create table");

                for i in 0..count {
                    db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 2))
                        .expect("Failed to insert");
                }
                black_box(count)
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parse,
    bench_parse_throughput,
    bench_optimize,
    bench_execute,
    bench_insert,
);
criterion_main!(benches);
