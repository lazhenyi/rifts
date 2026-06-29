//! Memory storage benchmarks — offset, log, dedupe, snapshot stores, key encoding.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rifts::storage::{
    DedupeStore, LogStore, MemoryDedupeStore, MemoryLogStore, MemoryOffsetStore,
    MemorySnapshotStore, OffsetStore, SnapshotStore, dedupe_key, dedupe_prefix, log_key,
    log_prefix, offset_key, offset_prefix, snapshot_key, snapshot_prefix,
};
use rifts::topic::{RetentionPolicy, TopicProfile, TopicStore};

use crate::common::{TOPIC_COUNTS, build_event_log_entry, payload_of, topic_names};

mod common;

fn bench_offset_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/offset_alloc");
    group.bench_function("same_topic", |b| {
        let store = MemoryOffsetStore::new();
        b.iter(|| black_box(store.alloc(black_box("bench.topic"))));
    });
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("many_topics", n), &n, |b, &n| {
            let store = MemoryOffsetStore::new();
            let names = topic_names(n);
            let mut i = 0;
            b.iter(|| {
                let name = &names[i % n];
                black_box(store.alloc(black_box(name)));
                i += 1;
            });
        });
    }
    group.finish();
}

fn bench_offset_head(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/offset_head");
    group.bench_function("hot", |b| {
        let store = MemoryOffsetStore::new();
        let _ = store.alloc("bench.topic");
        b.iter(|| black_box(store.head(black_box("bench.topic"))));
    });
    group.bench_function("cold", |b| {
        let store = MemoryOffsetStore::new();
        b.iter(|| black_box(store.head(black_box("nonexistent.topic"))));
    });
    group.finish();
}

fn bench_offset_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/offset_remove");
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("topics", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let store = MemoryOffsetStore::new();
                    let names = topic_names(n);
                    for name in &names {
                        let _ = store.alloc(name);
                    }
                    (store, names)
                },
                |(store, names)| {
                    for name in &names {
                        store.remove(black_box(name));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_log_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/log_append");
    let payloads: &[(usize, &str)] = &[(0, "0B"), (1024, "1KiB"), (16384, "16KiB")];
    let retentions = [
        ("none", RetentionPolicy::None),
        ("latest", RetentionPolicy::Latest),
        ("count100", RetentionPolicy::Count(100)),
        ("size1m", RetentionPolicy::Size(1024 * 1024)),
    ];
    for &(sz, label) in payloads {
        let payload = payload_of(sz);
        for (rname, retention) in retentions {
            group.bench_with_input(
                BenchmarkId::new(format!("{rname}/{label}"), sz),
                &payload,
                |b, payload| {
                    let store = MemoryLogStore::new();
                    let mut offset = 0i64;
                    b.iter(|| {
                        offset += 1;
                        let e = build_event_log_entry(offset, payload.clone());
                        store.append(black_box("bench.topic"), black_box(e), black_box(retention));
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_log_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/log_range");
    let sizes: &[(usize, &str)] = &[(10, "10"), (100, "100"), (1000, "1000")];
    for &(n, label) in sizes {
        group.bench_with_input(BenchmarkId::new("full", label), &n, |b, &n| {
            let store = MemoryLogStore::new();
            for i in 1..=n {
                let e = build_event_log_entry(i as i64, payload_of(64));
                store.append("bench.topic", e, RetentionPolicy::None);
            }
            b.iter(|| {
                let r = store.range(black_box("bench.topic"), black_box(1), black_box(n as i64));
                black_box(r);
            });
        });
    }
    group.finish();
}

fn bench_log_latest(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/log_latest");
    group.bench_function("some", |b| {
        let store = MemoryLogStore::new();
        store.append(
            "bench.topic",
            build_event_log_entry(1, payload_of(1024)),
            RetentionPolicy::None,
        );
        b.iter(|| black_box(store.latest(black_box("bench.topic"))));
    });
    group.bench_function("none", |b| {
        let store = MemoryLogStore::new();
        b.iter(|| black_box(store.latest(black_box("nonexistent.topic"))));
    });
    group.finish();
}

fn bench_dedupe_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/dedupe_check");
    let window = Duration::from_secs(30);
    group.bench_function("fresh", |b| {
        let store = MemoryDedupeStore::new();
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key-{i}");
            i += 1;
            black_box(store.check_and_record(
                black_box("bench.topic"),
                black_box(&key),
                black_box(window),
            ));
        });
    });
    group.bench_function("duplicate", |b| {
        let store = MemoryDedupeStore::new();
        let _ = store.check_and_record("bench.topic", "key-0", window);
        b.iter(|| {
            black_box(store.check_and_record(
                black_box("bench.topic"),
                black_box("key-0"),
                black_box(window),
            ));
        });
    });
    group.finish();
}

fn bench_dedupe_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/dedupe_sweep");
    for &n in &[100usize, 1000] {
        group.bench_with_input(BenchmarkId::new("expired", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let store = MemoryDedupeStore::new();
                    let short_window = Duration::from_millis(1);
                    for i in 0..n {
                        let _ = store.check_and_record(
                            "bench.topic",
                            &format!("key-{i}"),
                            short_window,
                        );
                    }
                    std::thread::sleep(Duration::from_millis(5));
                    store
                },
                |store| black_box(store.sweep()),
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_snapshot_capture(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/snapshot_capture");
    let payloads: &[(usize, &str)] = &[(0, "0B"), (1024, "1KiB"), (16384, "16KiB")];
    for &(sz, label) in payloads {
        group.bench_with_input(BenchmarkId::new("payload", label), &sz, |b, &sz| {
            let store = MemorySnapshotStore::new();
            let topic_store = TopicStore::new();
            let entry = topic_store
                .get_or_create("bench.topic", TopicProfile::default())
                .expect("create");
            entry.append(build_event_log_entry(1, payload_of(sz)));
            b.iter(|| {
                black_box(store.capture(
                    black_box("bench.topic"),
                    black_box(&topic_store),
                    black_box(None),
                ));
            });
        });
    }
    group.finish();
}

fn bench_snapshot_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/snapshot_get");
    group.bench_function("some", |b| {
        let store = MemorySnapshotStore::new();
        let topic_store = TopicStore::new();
        let entry = topic_store
            .get_or_create("bench.topic", TopicProfile::default())
            .expect("create");
        entry.append(build_event_log_entry(1, payload_of(1024)));
        let _ = store.capture("bench.topic", &topic_store, None);
        b.iter(|| black_box(store.get(black_box("bench.topic"))));
    });
    group.bench_function("none", |b| {
        let store = MemorySnapshotStore::new();
        b.iter(|| black_box(store.get(black_box("nonexistent.topic"))));
    });
    group.finish();
}

fn bench_key_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/key_encoding");
    group.bench_function("offset_key", |b| {
        b.iter(|| black_box(offset_key(black_box("bench.topic"))));
    });
    group.bench_function("offset_prefix", |b| {
        b.iter(|| black_box(offset_prefix(black_box("bench.topic"))));
    });
    group.bench_function("log_key", |b| {
        b.iter(|| black_box(log_key(black_box("bench.topic"), black_box(42i64))));
    });
    group.bench_function("log_prefix", |b| {
        b.iter(|| black_box(log_prefix(black_box("bench.topic"))));
    });
    group.bench_function("dedupe_key", |b| {
        b.iter(|| black_box(dedupe_key(black_box("bench.topic"), black_box("msg-1"))));
    });
    group.bench_function("dedupe_prefix", |b| {
        b.iter(|| black_box(dedupe_prefix(black_box("bench.topic"))));
    });
    group.bench_function("snapshot_key", |b| {
        b.iter(|| black_box(snapshot_key(black_box("bench.topic"), black_box("snap-1"))));
    });
    group.bench_function("snapshot_prefix", |b| {
        b.iter(|| black_box(snapshot_prefix(black_box("bench.topic"))));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_offset_alloc,
    bench_offset_head,
    bench_offset_remove,
    bench_log_append,
    bench_log_range,
    bench_log_latest,
    bench_dedupe_check,
    bench_dedupe_sweep,
    bench_snapshot_capture,
    bench_snapshot_get,
    bench_key_encoding,
);
criterion_main!(benches);
