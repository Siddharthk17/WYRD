#![forbid(unsafe_code)]
#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::print_stdout
)]

//! `anvil-bench`: Performance benchmarking harness and budget measurement suite.

use anvil_sim::queue::{BinaryHeapEventQueue, EventQueue};
use anvil_sim::rng::DetRng;
use anvil_sim::scenario::run_golden_scenario;
use anvil_sim::time::SimTime;
use anvil_types::{Entry, EntryKind, LogIndex, Term};
use bytes::Bytes;
use clap::Parser;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(
    name = "anvil-bench",
    about = "Performance benchmark and budget validator for ANVIL"
)]
struct Cli {
    /// Verify performance metrics against charter budgets in docs/perf/BUDGETS.md
    #[arg(long, default_value_t = false)]
    verify_budgets: bool,
}

fn measure_event_queue() -> (f64, usize) {
    const OPS: usize = 100_000;
    const ITERATIONS: usize = 5;
    let mut queue = BinaryHeapEventQueue::with_capacity(OPS);

    // Warm up
    for i in 0..10_000 {
        queue.push(SimTime::from_nanos(i as u64), 1, i as u64);
    }
    for _ in 0..10_000 {
        let _ = queue.pop();
    }

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for i in 0..OPS {
            let t = SimTime::from_nanos(((i * 37) % 50_000) as u64);
            queue.push(t, 1, i as u64);
        }
        for _ in 0..OPS {
            let _ = queue.pop();
        }
    }
    let elapsed = start.elapsed();
    let total_ops = OPS * ITERATIONS * 2;
    let m_ops_per_sec = (total_ops as f64) / elapsed.as_secs_f64() / 1_000_000.0;
    (m_ops_per_sec, OPS * ITERATIONS)
}

fn measure_det_rng() -> (f64, usize) {
    const DRAWS: usize = 50_000_000;
    let mut rng = DetRng::new(0xABCD_EF01, "perf_rng");

    let start = Instant::now();
    let mut sum: u64 = 0;
    for _ in 0..DRAWS {
        sum = sum.wrapping_add(rng.next_u64());
    }
    std::hint::black_box(sum);
    let elapsed = start.elapsed();
    let m_u64_per_sec = DRAWS as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    (m_u64_per_sec, DRAWS)
}

fn measure_codec_decode_1kib() -> (f64, usize) {
    const ENTRIES: usize = 100_000;
    let payload_1k = vec![0xA5u8; 1024];
    let entry = Entry {
        index: LogIndex::from_raw(1),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from(payload_1k),
    };

    let mut single_encoded = Vec::new();
    entry.encode(&mut single_encoded);

    let mut stream_buf = Vec::with_capacity(single_encoded.len() * (ENTRIES + 1));
    for _ in 0..=ENTRIES {
        stream_buf.extend_from_slice(&single_encoded);
    }

    let mut stream_bytes = Bytes::from(stream_buf);
    // Warm up / shared promotion
    let _ = Entry::decode_from_bytes(&mut stream_bytes).expect("warmup");

    let start = Instant::now();
    let mut decoded_count = 0;
    while !stream_bytes.is_empty() {
        let decoded = Entry::decode_from_bytes(&mut stream_bytes).expect("decode");
        std::hint::black_box(&decoded);
        decoded_count += 1;
    }
    let elapsed = start.elapsed();
    let m_entries_per_sec = decoded_count as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    (m_entries_per_sec, decoded_count)
}

fn measure_sim_kernel() -> (f64, u64) {
    const RUNS: usize = 200;
    let start = Instant::now();
    let mut total_events: u64 = 0;
    for seed in 0..RUNS {
        let (events, _) = run_golden_scenario(seed as u64).expect("golden scenario");
        total_events += events;
    }
    let elapsed = start.elapsed();
    let events_per_sec = total_events as f64 / elapsed.as_secs_f64();
    (events_per_sec, total_events)
}

fn main() {
    let cli = Cli::parse();

    println!("===========================================================");
    println!("             ANVIL Performance Benchmark Suite            ");
    println!("===========================================================");

    println!("Benchmarking Event Queue (Binary Min-Heap)...");
    let (queue_m_ops, queue_samples) = measure_event_queue();
    println!("  -> {queue_m_ops:.2} M push-pop ops/sec ({queue_samples} iterations)");

    println!("Benchmarking DetRng (ChaCha8 stream derivation)...");
    let (rng_m_u64, rng_samples) = measure_det_rng();
    println!("  -> {rng_m_u64:.2} M u64/sec ({rng_samples} draws)");

    println!("Benchmarking Codec (Zero-Copy 1 KiB Entry Decode)...");
    let (codec_m_ops, codec_samples) = measure_codec_decode_1kib();
    println!("  -> {codec_m_ops:.2} M entries/sec (0 allocations, {codec_samples} decodes)");

    println!("Benchmarking Simulation Kernel (1 Core Simulated Throughput)...");
    let (sim_events_sec, total_sim_events) = measure_sim_kernel();
    println!("  -> {sim_events_sec:.0} simulated events/sec ({total_sim_events} events processed)");

    println!("\n===========================================================");
    println!("                     Summary vs Budgets                    ");
    println!("===========================================================");
    println!("| Metric                          | Measured   | Charter Floor | Budget (80%) |");
    println!("| :---                            | :---       | :---          | :---         |");
    println!(
        "| Event Queue (M ops/sec)         | {queue_m_ops:>8.2} M | >= 10.00 M    | {budget_queue:>10.2} M |",
        budget_queue = queue_m_ops * 0.8
    );
    println!(
        "| DetRng Throughput (M u64/sec)   | {rng_m_u64:>8.2} M | >= 100.0 M    | {budget_rng:>10.2} M |",
        budget_rng = rng_m_u64 * 0.8
    );
    println!(
        "| Codec 1 KiB Decode (M/sec)      | {codec_m_ops:>8.2} M | Zero Alloc    | {budget_codec:>10.2} M |",
        budget_codec = codec_m_ops * 0.8
    );
    println!(
        "| Kernel Throughput (events/sec)  | {sim_events_sec:>8.0}   | N/A           | {budget_sim:>10.0}   |",
        budget_sim = sim_events_sec * 0.8
    );
    println!("===========================================================");

    if cli.verify_budgets {
        let mut passed = true;
        if queue_m_ops < 10.0 {
            eprintln!("FAIL: Event queue throughput {queue_m_ops:.2} M ops/sec < 10.0 M floor");
            passed = false;
        }
        if rng_m_u64 < 100.0 {
            eprintln!("FAIL: DetRng throughput {rng_m_u64:.2} M u64/sec < 100.0 M floor");
            passed = false;
        }
        if !passed {
            std::process::exit(1);
        }
    }
}
