//! Flow control benchmarks — backpressure controller and rate limiter.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rifts::Priority;
use rifts::flow::{
    BackpressureAction, BackpressureController, BackpressureStrategy, RateLimitTable, RateLimiter,
    is_volatile,
};

mod common;

fn bench_bp_enqueue_accept(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/backpressure_enqueue");
    let capacities: &[(usize, &str)] = &[(1024, "1KiB"), (65536, "64KiB"), (1048576, "1MiB")];
    for &(cap, label) in capacities {
        group.bench_with_input(BenchmarkId::new("accept", label), &cap, |b, &cap| {
            let bp = BackpressureController::new(cap);
            let chunk = cap / 4;
            b.iter(|| {
                let act = bp.try_enqueue(black_box(chunk));
                if let BackpressureAction::Accept = act {
                    bp.release(black_box(chunk));
                }
                black_box(act);
            });
        });
    }
    group.finish();
}

fn bench_bp_overloaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/backpressure_overloaded");
    let strategies = [
        ("pause", BackpressureStrategy::Pause),
        ("drop_volatile", BackpressureStrategy::DropVolatile),
        ("coalesce", BackpressureStrategy::CoalesceState),
        ("downgrade", BackpressureStrategy::Downgrade),
        ("disconnect", BackpressureStrategy::Disconnect),
        ("snapshot", BackpressureStrategy::SnapshotLater),
    ];
    for (name, strat) in strategies {
        group.bench_with_input(BenchmarkId::new("full", name), &strat, |b, &strat| {
            let bp = BackpressureController::new(1024);
            bp.set_strategy(strat);
            // Fill the queue so the next enqueue hits the slow path.
            let _ = bp.try_enqueue(1024);
            b.iter(|| {
                let act = bp.try_enqueue(black_box(16));
                black_box(act);
            });
        });
    }
    group.finish();
}

fn bench_bp_release(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/backpressure_release");
    group.bench_function("release", |b| {
        let bp = BackpressureController::new(65536);
        for _ in 0..32 {
            let _ = bp.try_enqueue(1024);
        }
        b.iter(|| {
            bp.release(black_box(1024));
        });
    });
    group.finish();
}

fn bench_bp_counters(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/backpressure_counters");
    group.bench_function("current_bytes", |b| {
        let bp = BackpressureController::new(65536);
        let _ = bp.try_enqueue(1024);
        b.iter(|| black_box(bp.current_bytes()));
    });
    group.bench_function("is_overloaded", |b| {
        let bp = BackpressureController::new(65536);
        let _ = bp.try_enqueue(65536);
        b.iter(|| black_box(bp.is_overloaded()));
    });
    group.bench_function("available", |b| {
        let bp = BackpressureController::new(65536);
        b.iter(|| black_box(bp.available()));
    });
    group.finish();
}

fn bench_rate_limiter(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/rate_limiter");
    group.bench_function("try_take_hit", |b| {
        let rl = RateLimiter::new(1_000_000, 10);
        b.iter(|| black_box(rl.try_take()));
    });
    group.bench_function("try_take_n", |b| {
        let rl = RateLimiter::new(1_000_000, 100);
        b.iter(|| black_box(rl.try_take_n(black_box(5))));
    });
    group.bench_function("try_take_exhausted", |b| {
        let rl = RateLimiter::new(1, 1);
        let _ = rl.try_take();
        b.iter(|| black_box(rl.try_take()));
    });
    group.finish();
}

fn bench_rate_limit_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/rate_limit_table");
    group.bench_function("get_existing", |b| {
        let table = RateLimitTable::new();
        let _ = table.get("conn1/topic1", 100, 10);
        b.iter(|| black_box(table.get(black_box("conn1/topic1"), 100, 10)));
    });
    group.bench_function("get_new", |b| {
        let table = RateLimitTable::new();
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            let key = format!("conn{i}/topic");
            black_box(table.get(black_box(&key), 100, 10))
        });
    });
    group.finish();
}

fn bench_is_volatile(c: &mut Criterion) {
    let mut group = c.benchmark_group("flow/is_volatile");
    let cases = [
        ("none", None),
        ("background", Some(Priority::Background)),
        ("volatile", Some(Priority::Volatile)),
        ("normal", Some(Priority::Normal)),
        ("critical", Some(Priority::Critical)),
    ];
    for (name, p) in cases {
        group.bench_with_input(BenchmarkId::new("check", name), &p, |b, p| {
            b.iter(|| black_box(is_volatile(black_box(*p))));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_bp_enqueue_accept,
    bench_bp_overloaded,
    bench_bp_release,
    bench_bp_counters,
    bench_rate_limiter,
    bench_rate_limit_table,
    bench_is_volatile,
);
criterion_main!(benches);
