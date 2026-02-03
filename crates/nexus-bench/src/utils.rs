//! Benchmark utilities and helpers.

use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Generates random string data for benchmarks.
pub fn random_string(rng: &mut StdRng, len: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

/// Generates a batch of random key-value pairs.
pub fn generate_kv_pairs(
    count: usize,
    key_size: usize,
    value_size: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count)
        .map(|_| {
            let key: Vec<u8> = (0..key_size).map(|_| rng.gen()).collect();
            let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
            (key, value)
        })
        .collect()
}

/// Generates sequential keys for benchmarks.
pub fn generate_sequential_keys(count: usize, prefix: &str) -> Vec<String> {
    (0..count).map(|i| format!("{}{:08}", prefix, i)).collect()
}

/// Generates random user records for SQL benchmarks.
pub fn generate_user_records(count: usize) -> Vec<(i32, String, i32)> {
    let mut rng = StdRng::seed_from_u64(42);
    let names = [
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
    ];

    (0..count as i32)
        .map(|i| {
            let name_idx = rng.gen_range(0..names.len());
            let suffix: u32 = rng.gen_range(0..10000);
            let name = format!("{}_{}", names[name_idx], suffix);
            let age = rng.gen_range(18..80);
            (i + 1, name, age)
        })
        .collect()
}
