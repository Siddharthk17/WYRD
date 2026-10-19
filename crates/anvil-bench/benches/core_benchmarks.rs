#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::cast_precision_loss,
    clippy::items_after_statements
)]

//! Criterion benchmarks for ANVIL event queue, `DetRng`, and zero-copy binary codec.

use anvil_sim::queue::{BinaryHeapEventQueue, EventQueue};
use anvil_sim::rng::DetRng;
use anvil_sim::time::SimTime;
use anvil_types::{Entry, EntryKind, LogIndex, Term};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

fn bench_event_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_queue");
    const BATCH_SIZE: u64 = 10_000;
    group.throughput(Throughput::Elements(BATCH_SIZE));

    group.bench_function("binary_heap_push_pop", |b| {
        let mut queue = BinaryHeapEventQueue::new();
        b.iter(|| {
            for i in 0..BATCH_SIZE {
                let t = SimTime::from_nanos((i * 37) % 50_000);
                queue.push(t, 1, i);
            }
            for _ in 0..BATCH_SIZE {
                let ev = queue.pop();
                black_box(ev);
            }
        });
    });
    group.finish();
}

fn bench_det_rng(c: &mut Criterion) {
    let mut group = c.benchmark_group("det_rng");
    const BATCH_SIZE: u64 = 100_000;
    group.throughput(Throughput::Elements(BATCH_SIZE));

    group.bench_function("next_u64", |b| {
        let mut rng = DetRng::new(0x1234_5678, "benchmark");
        b.iter(|| {
            let mut sum: u64 = 0;
            for _ in 0..BATCH_SIZE {
                sum = sum.wrapping_add(rng.next_u64());
            }
            black_box(sum)
        });
    });

    group.bench_function("gen_range", |b| {
        let mut rng = DetRng::new(0x1234_5678, "benchmark");
        b.iter(|| {
            let mut sum: u64 = 0;
            for _ in 0..BATCH_SIZE {
                sum = sum.wrapping_add(rng.gen_range(5..500));
            }
            black_box(sum)
        });
    });
    group.finish();
}

fn bench_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec");
    const PAYLOAD_LEN: usize = 1024;
    group.throughput(Throughput::Bytes(PAYLOAD_LEN as u64));

    let payload_1k = vec![0xEEu8; PAYLOAD_LEN];
    let entry = Entry {
        index: LogIndex::from_raw(500),
        term: Term::from_raw(2),
        kind: EntryKind::Normal,
        payload: Bytes::from(payload_1k),
    };

    let mut single_encoded = Vec::new();
    entry.encode(&mut single_encoded);

    // Create a 100-entry shared buffer
    let mut stream_buf = Vec::with_capacity(single_encoded.len() * 100);
    for _ in 0..100 {
        stream_buf.extend_from_slice(&single_encoded);
    }
    let wire_bytes_template = Bytes::from(stream_buf);

    group.bench_function(BenchmarkId::new("decode_entry_1kib", PAYLOAD_LEN), |b| {
        b.iter(|| {
            let mut cursor = wire_bytes_template.clone();
            while !cursor.is_empty() {
                let decoded = Entry::decode_from_bytes(&mut cursor).expect("decode");
                black_box(decoded);
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_event_queue, bench_det_rng, bench_codec);
criterion_main!(benches);
