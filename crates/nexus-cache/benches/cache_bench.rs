//! Cache benchmarks.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use nexus_cache::arc::ArcCache;
use nexus_cache::bloom::BloomFilter;
use nexus_cache::lru::LruCache;

fn lru_insert_benchmark(c: &mut Criterion) {
    c.bench_function("lru_insert_1000", |b| {
        b.iter(|| {
            let mut cache = LruCache::new(1000);
            for i in 0..1000 {
                cache.insert(i, i * 2);
            }
            black_box(cache.len())
        })
    });
}

fn lru_get_benchmark(c: &mut Criterion) {
    let mut cache = LruCache::new(1000);
    for i in 0..1000 {
        cache.insert(i, i * 2);
    }

    c.bench_function("lru_get_1000", |b| {
        b.iter(|| {
            for i in 0..1000 {
                black_box(cache.get(&i));
            }
        })
    });
}

fn arc_insert_benchmark(c: &mut Criterion) {
    c.bench_function("arc_insert_1000", |b| {
        b.iter(|| {
            let mut cache = ArcCache::new(1000);
            for i in 0..1000 {
                cache.insert(i, i * 2);
            }
            black_box(cache.len())
        })
    });
}

fn bloom_insert_benchmark(c: &mut Criterion) {
    c.bench_function("bloom_insert_10000", |b| {
        b.iter(|| {
            let mut filter = BloomFilter::with_rate(10000, 0.01);
            for i in 0..10000 {
                filter.insert(&i);
            }
            black_box(filter.count())
        })
    });
}

fn bloom_contains_benchmark(c: &mut Criterion) {
    let mut filter = BloomFilter::with_rate(10000, 0.01);
    for i in 0..10000 {
        filter.insert(&i);
    }

    c.bench_function("bloom_contains_10000", |b| {
        b.iter(|| {
            let mut found = 0;
            for i in 0..10000 {
                if filter.contains(&i) {
                    found += 1;
                }
            }
            black_box(found)
        })
    });
}

criterion_group!(
    benches,
    lru_insert_benchmark,
    lru_get_benchmark,
    arc_insert_benchmark,
    bloom_insert_benchmark,
    bloom_contains_benchmark,
);
criterion_main!(benches);
