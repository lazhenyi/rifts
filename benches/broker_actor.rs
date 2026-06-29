//! Actor broker benchmarks — ActorBroker publish, subscribe, replay, TopicRegistry.

use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rifts::actor::TopicRegistry;
use rifts::broker::actor_broker::ActorBroker;
use rifts::broker::{Broker, SubscribeIntent};
use rifts::storage::{MemoryDedupeStore, MemoryLogStore, MemoryOffsetStore, MemorySnapshotStore};
use rifts::topic::TopicProfile;

use crate::common::{
    PAYLOAD_SIZE_LABELS, PAYLOAD_SIZES, SUBSCRIBER_COUNTS, TOPIC_COUNTS, atomic_sink,
    build_data_frame, payload_of, topic_names,
};

mod common;

fn build_actor_broker() -> Arc<dyn Broker> {
    let offsets = Arc::new(MemoryOffsetStore::new());
    let log = Arc::new(MemoryLogStore::new());
    let dedupe = Arc::new(MemoryDedupeStore::new());
    let snapshots = Arc::new(MemorySnapshotStore::new());
    let registry = TopicRegistry::new(
        offsets,
        log,
        dedupe,
        snapshots,
        TopicProfile::default(),
        Duration::from_secs(30),
    );
    Arc::new(ActorBroker::new(Arc::new(registry)))
}

fn bench_publish_no_subs(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_actor/publish_no_subs");
    let payloads: Vec<(String, _)> = PAYLOAD_SIZES
        .iter()
        .zip(PAYLOAD_SIZE_LABELS.iter())
        .map(|(&sz, &label)| (label.to_string(), build_data_frame(payload_of(sz), 1)))
        .collect();
    let rt = common::runtime();
    for (label, frame) in &payloads {
        group.bench_with_input(BenchmarkId::new("payload", label), frame, |b, frame| {
            let broker = build_actor_broker();
            let mut frame_id = 1u64;
            b.to_async(&rt).iter(|| {
                let mut f = frame.clone();
                f.frame_id = frame_id;
                frame_id += 1;
                f.message_id = Some(format!("msg-{frame_id}"));
                let broker = broker.clone();
                async move {
                    let _ = broker.publish(black_box(&f)).await;
                    black_box(());
                }
            });
        });
    }
    group.finish();
}

fn bench_publish_with_subs(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_actor/publish_with_subs");
    let payload = payload_of(1024);
    let rt = common::runtime();
    for &n in SUBSCRIBER_COUNTS {
        group.bench_with_input(BenchmarkId::new("subscribers", n), &n, |b, &n| {
            let broker = build_actor_broker();
            for i in 0..n {
                let _ = broker.subscribe(
                    "bench.topic.0",
                    SubscribeIntent::Live,
                    atomic_sink(i as u64),
                );
            }
            let mut frame_id = 1u64;
            b.to_async(&rt).iter(|| {
                let f = build_data_frame(payload.clone(), frame_id);
                frame_id += 1;
                let broker = broker.clone();
                async move {
                    let _ = broker.publish(black_box(&f)).await;
                    black_box(());
                }
            });
        });
    }
    group.finish();
}

fn bench_publish_multi_topic(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_actor/publish_multi_topic");
    let payload = payload_of(256);
    let rt = common::runtime();
    for &n in TOPIC_COUNTS {
        group.bench_with_input(BenchmarkId::new("topics", n), &n, |b, &n| {
            let broker = build_actor_broker();
            let names = topic_names(n);
            let mut i = 0u64;
            b.to_async(&rt).iter(|| {
                let topic = names[(i as usize) % n].clone();
                let f = build_data_frame(payload.clone(), i + 1);
                i += 1;
                let broker = broker.clone();
                let topic = black_box(topic);
                async move {
                    let _ = broker.publish(&f).await;
                    black_box(topic);
                }
            });
        });
    }
    group.finish();
}

fn bench_subscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_actor/subscribe");
    let rt = common::runtime();
    for &n in SUBSCRIBER_COUNTS {
        group.bench_with_input(BenchmarkId::new("count", n), &n, |b, &n| {
            b.iter_batched(
                || build_actor_broker(),
                |broker| {
                    rt.block_on(async {
                        for i in 0..n {
                            let _ = broker
                                .subscribe(
                                    "bench.topic.0",
                                    SubscribeIntent::Live,
                                    atomic_sink(i as u64),
                                )
                                .await;
                        }
                        black_box(broker);
                    });
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_actor/replay");
    let sizes: &[(usize, &str)] = &[(10, "10"), (100, "100"), (1000, "1000")];
    let rt = common::runtime();
    for &(n, label) in sizes {
        group.bench_with_input(BenchmarkId::new("range", label), &n, |b, &n| {
            let broker = build_actor_broker();
            rt.block_on(async {
                for i in 1..=n {
                    let f = build_data_frame(payload_of(64), i as u64);
                    let _ = broker.publish(&f).await;
                }
            });
            b.to_async(&rt).iter(|| {
                let broker = broker.clone();
                async move {
                    let r = broker
                        .replay(
                            black_box("bench.topic.0"),
                            black_box(1),
                            black_box(n as i64),
                        )
                        .await;
                    black_box(r);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_publish_no_subs,
    bench_publish_with_subs,
    bench_publish_multi_topic,
    bench_subscribe,
    bench_replay,
);
criterion_main!(benches);
