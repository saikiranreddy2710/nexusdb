//! Storage engine benchmarks for NexusDB.
//!
//! Benchmarks for:
//! - SageTree insert operations
//! - SageTree get operations
//! - SageTree range scans
//! - Sequential vs random access patterns

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexus_common::types::{Key, Value};
use nexus_storage::sagetree::SageTree;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Generate random key-value pairs.
fn generate_kv_pairs(count: usize, key_size: usize, value_size: usize) -> Vec<(Key, Value)> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count)
        .map(|_| {
            let key: Vec<u8> = (0..key_size).map(|_| rng.gen()).collect();
            let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
            (Key::from(key), Value::from(value))
        })
        .collect()
}

/// Generate sequential keys for benchmarks.
fn generate_sequential_pairs(count: usize, value_size: usize) -> Vec<(Key, Value)> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count)
        .map(|i| {
            let key = format!("key_{:08}", i);
            let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
            (Key::from(key.into_bytes()), Value::from(value))
        })
        .collect()
}

/// Benchmark sequential inserts into SageTree.
fn bench_insert_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/insert_sequential");

    for size in [1000, 10_000, 50_000].iter() {
        let pairs = generate_sequential_pairs(*size, 100);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let tree = SageTree::new();
                for (key, value) in &pairs {
                    tree.insert(key.clone(), value.clone()).unwrap();
                }
                black_box(tree.len())
            });
        });
    }

    group.finish();
}

/// Benchmark random inserts into SageTree.
fn bench_insert_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/insert_random");

    for size in [1000, 10_000, 50_000].iter() {
        let pairs = generate_kv_pairs(*size, 16, 100);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let tree = SageTree::new();
                for (key, value) in &pairs {
                    tree.insert(key.clone(), value.clone()).unwrap();
                }
                black_box(tree.len())
            });
        });
    }

    group.finish();
}

/// Benchmark point lookups in SageTree.
fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/get");

    for size in [1000, 10_000, 50_000].iter() {
        // Setup: insert data first
        let pairs = generate_sequential_pairs(*size, 100);
        let tree = SageTree::new();
        for (key, value) in &pairs {
            tree.insert(key.clone(), value.clone()).unwrap();
        }

        // Prepare lookup keys
        let lookup_keys: Vec<_> = pairs.iter().map(|(k, _)| k.clone()).collect();

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let mut found = 0;
                for key in &lookup_keys {
                    if tree.get(key).unwrap().is_some() {
                        found += 1;
                    }
                }
                black_box(found)
            });
        });
    }

    group.finish();
}

/// Benchmark range scans in SageTree.
fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/scan");

    // Setup: insert data
    let pairs = generate_sequential_pairs(10_000, 100);
    let tree = SageTree::new();
    for (key, value) in &pairs {
        tree.insert(key.clone(), value.clone()).unwrap();
    }

    use nexus_storage::sagetree::KeyRange;

    // Benchmark different scan sizes
    for scan_size in [100, 500, 1000].iter() {
        group.throughput(Throughput::Elements(*scan_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(scan_size),
            scan_size,
            |b, &size| {
                b.iter(|| {
                    let results = tree.scan_with_limit(KeyRange::all(), size).unwrap();
                    black_box(results.len())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark mixed workload (reads and writes).
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/mixed");

    // 80% reads, 20% writes workload
    let initial_data = generate_sequential_pairs(10_000, 100);
    let new_data = generate_kv_pairs(2000, 16, 100);

    group.bench_function("80read_20write", |b| {
        b.iter(|| {
            let tree = SageTree::new();

            // Insert initial data
            for (key, value) in &initial_data {
                tree.insert(key.clone(), value.clone()).unwrap();
            }

            // Mixed workload: 80% reads, 20% writes
            let mut rng = StdRng::seed_from_u64(123);
            let mut read_count = 0;
            let mut write_count = 0;

            for i in 0..10_000 {
                if rng.gen_ratio(80, 100) {
                    // Read
                    let idx = i % initial_data.len();
                    let _ = tree.get(&initial_data[idx].0);
                    read_count += 1;
                } else {
                    // Write
                    let idx = write_count % new_data.len();
                    let _ = tree.insert(new_data[idx].0.clone(), new_data[idx].1.clone());
                    write_count += 1;
                }
            }

            black_box((read_count, write_count))
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_sequential,
    bench_insert_random,
    bench_get,
    bench_scan,
    bench_mixed_workload,
);
criterion_main!(benches);
