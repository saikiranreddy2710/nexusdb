//! End-to-end database benchmarks for NexusDB.
//!
//! Benchmarks for:
//! - Full database operations (CRUD)
//! - Concurrent operations
//! - Client-server communication

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexus_server::database::Database;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Benchmark full CRUD cycle.
fn bench_crud_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("database/crud");

    group.bench_function("create_insert_select_drop", |b| {
        b.iter(|| {
            let db = Database::open_memory().expect("Failed to create database");

            // CREATE
            db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
                .expect("CREATE failed");

            // INSERT
            db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
                .expect("INSERT failed");
            db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
                .expect("INSERT failed");
            db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
                .expect("INSERT failed");

            // SELECT
            let result = db.execute("SELECT * FROM users").expect("SELECT failed");

            // UPDATE
            db.execute("UPDATE users SET age = 31 WHERE id = 1")
                .expect("UPDATE failed");

            // DELETE
            db.execute("DELETE FROM users WHERE id = 2")
                .expect("DELETE failed");

            // DROP
            db.execute("DROP TABLE users").expect("DROP failed");

            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark database with varying data sizes.
fn bench_data_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("database/data_size");

    for size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("insert_then_select", size),
            size,
            |b, &size| {
                b.iter(|| {
                    let db = Database::open_memory().expect("Failed to create database");
                    db.execute("CREATE TABLE t (id INT PRIMARY KEY, data TEXT)")
                        .expect("CREATE failed");

                    // Insert all rows
                    for i in 0..size {
                        db.execute(&format!("INSERT INTO t VALUES ({}, 'data_{}')", i, i))
                            .expect("INSERT failed");
                    }

                    // Select all rows
                    let result = db.execute("SELECT * FROM t").expect("SELECT failed");

                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark mixed read-write workloads.
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("database/mixed_workload");

    // Setup: pre-populated database
    fn setup_db(rows: usize) -> Database {
        let db = Database::open_memory().expect("Failed to create database");
        db.execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, product TEXT, quantity INT, price INT)",
        )
        .expect("CREATE failed");

        for i in 0..rows {
            db.execute(&format!(
                "INSERT INTO orders VALUES ({}, 'product_{}', {}, {})",
                i,
                i % 100,
                (i % 10) + 1,
                ((i % 50) + 1) * 10
            ))
            .expect("INSERT failed");
        }
        db
    }

    // 90% read, 10% write
    group.bench_function("90r_10w_1000rows", |b| {
        let db = setup_db(1000);
        let mut rng = StdRng::seed_from_u64(42);
        let mut op_counter = 0;

        b.iter(|| {
            op_counter += 1;
            if rng.gen_ratio(90, 100) {
                // Read operation
                let id = rng.gen_range(0..1000);
                let result = db.execute(&format!("SELECT * FROM orders WHERE id = {}", id));
                black_box(result)
            } else {
                // Write operation
                let id = rng.gen_range(0..1000);
                let new_qty = rng.gen_range(1..20);
                let result = db.execute(&format!(
                    "UPDATE orders SET quantity = {} WHERE id = {}",
                    new_qty, id
                ));
                black_box(result)
            }
        });
    });

    // 50% read, 50% write
    group.bench_function("50r_50w_1000rows", |b| {
        let db = setup_db(1000);
        let mut rng = StdRng::seed_from_u64(42);
        let mut insert_counter = 1000;

        b.iter(|| {
            if rng.gen_ratio(50, 100) {
                // Read operation
                let id = rng.gen_range(0..1000);
                let result = db.execute(&format!("SELECT * FROM orders WHERE id = {}", id));
                black_box(result)
            } else {
                // Write operation (insert new row)
                insert_counter += 1;
                let result = db.execute(&format!(
                    "INSERT INTO orders VALUES ({}, 'new_product', 1, 100)",
                    insert_counter
                ));
                black_box(result)
            }
        });
    });

    group.finish();
}

/// Benchmark query complexity.
fn bench_query_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("database/query_complexity");

    // Setup database with test data
    let db = Database::open_memory().expect("Failed to create database");
    db.execute(
        "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category INT, price INT, stock INT)",
    )
    .expect("CREATE failed");

    let mut rng = StdRng::seed_from_u64(42);
    for i in 0..1000 {
        db.execute(&format!(
            "INSERT INTO products VALUES ({}, 'product_{}', {}, {}, {})",
            i,
            i,
            rng.gen_range(1..10),    // category
            rng.gen_range(10..1000), // price
            rng.gen_range(0..100)    // stock
        ))
        .expect("INSERT failed");
    }

    // Simple point lookup
    group.bench_function("point_lookup", |b| {
        b.iter(|| {
            let result = db.execute("SELECT * FROM products WHERE id = 500");
            black_box(result)
        });
    });

    // Range scan
    group.bench_function("range_scan", |b| {
        b.iter(|| {
            let result = db.execute("SELECT * FROM products WHERE id > 400 AND id < 600");
            black_box(result)
        });
    });

    // Full table scan with filter
    group.bench_function("full_scan_filter", |b| {
        b.iter(|| {
            let result = db.execute("SELECT * FROM products WHERE price > 500");
            black_box(result)
        });
    });

    // Projection (fewer columns)
    group.bench_function("projection", |b| {
        b.iter(|| {
            let result = db.execute("SELECT name, price FROM products LIMIT 100");
            black_box(result)
        });
    });

    // ORDER BY
    group.bench_function("order_by", |b| {
        b.iter(|| {
            let result = db.execute("SELECT * FROM products ORDER BY price LIMIT 50");
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark table operations.
fn bench_table_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("database/table_ops");

    // CREATE TABLE
    group.bench_function("create_table", |b| {
        b.iter_with_setup(
            || Database::open_memory().expect("Failed to create database"),
            |db| {
                let result =
                    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT, c BOOLEAN)");
                black_box(result)
            },
        );
    });

    // DROP TABLE
    group.bench_function("drop_table", |b| {
        b.iter_with_setup(
            || {
                let db = Database::open_memory().expect("Failed to create database");
                db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
                    .expect("CREATE failed");
                db
            },
            |db| {
                let result = db.execute("DROP TABLE t");
                black_box(result)
            },
        );
    });

    // CREATE IF NOT EXISTS (table exists)
    group.bench_function("create_if_not_exists", |b| {
        let db = Database::open_memory().expect("Failed to create database");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .expect("CREATE failed");

        b.iter(|| {
            let result = db.execute("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)");
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark database initialization.
fn bench_database_init(c: &mut Criterion) {
    let mut group = c.benchmark_group("database/init");

    group.bench_function("open_memory", |b| {
        b.iter(|| {
            let db = Database::open_memory();
            black_box(db)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_database_init,
    bench_crud_cycle,
    bench_data_sizes,
    bench_mixed_workload,
    bench_query_complexity,
    bench_table_operations,
);
criterion_main!(benches);
