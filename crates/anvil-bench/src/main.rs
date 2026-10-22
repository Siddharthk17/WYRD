#![forbid(unsafe_code)]
#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::too_many_lines
)]

//! `anvil-bench`: Performance benchmarking harness and budget measurement suite.

use anvil_raft::{Input, Output, RaftConfig, RaftCore, RecoveredState};
use anvil_sim::queue::{BinaryHeapEventQueue, EventQueue};
use anvil_sim::rng::DetRng;
use anvil_sim::scenario::run_golden_scenario;
use anvil_sim::time::SimTime;
use anvil_types::{Entry, EntryKind, LogIndex, NodeId, Term};
use bytes::Bytes;
use clap::Parser;
use rand_chacha::rand_core::SeedableRng;
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

fn measure_raft_replication() -> (f64, usize) {
    const ENTRIES: usize = 50_000;
    let node1 = NodeId::new(1).expect("id 1");
    let node2 = NodeId::new(2).expect("id 2");
    let node3 = NodeId::new(3).expect("id 3");

    let make_node = |id: NodeId, peers: &[NodeId]| -> RaftCore {
        let mut peer_set = std::collections::BTreeSet::new();
        for &p in peers {
            peer_set.insert(p);
        }
        let config = RaftConfig {
            node_id: id,
            peers: peer_set,
            election_timeout_ticks: 1_000_000,
            heartbeat_interval_ticks: 1_000_000,
            max_inflight_messages: 256,
            max_bytes_per_message: 64 * 1024,
            pre_vote_enabled: true,
            check_quorum: false,
        };
        let rng = rand_chacha::ChaCha8Rng::seed_from_u64(id.get());
        RaftCore::new(config, RecoveredState::default(), rng)
    };

    let mut leader = make_node(node1, &[node2, node3]);
    let mut follower2 = make_node(node2, &[node1, node3]);
    let mut follower3 = make_node(node3, &[node1, node2]);

    let mut leader_out = Output::new();
    let mut f2_out = Output::new();
    let mut f3_out = Output::new();

    // Leader election warmup
    leader.testing_become_leader(&mut leader_out);
    let mut initial_ops = std::mem::take(&mut leader_out.persist_ops);
    for op in initial_ops.drain(..) {
        leader.step(Input::Persisted { token: op.token() }, &mut leader_out);
    }
    leader_out.clear();

    let payload =
        Bytes::from_static(b"benchmark_raft_replicated_command_payload_64_bytes_zero_copy_slice");

    let mut leader_ops_buf = Vec::new();
    let mut leader_msgs_buf = Vec::new();
    let mut f2_ops_buf = Vec::new();
    let mut f2_msgs_buf = Vec::new();
    let mut f3_ops_buf = Vec::new();
    let mut f3_msgs_buf = Vec::new();

    // Warm-up 100 entries
    for _ in 0..100 {
        leader_out.clear();
        leader.step(
            Input::Propose {
                payload: payload.clone(),
            },
            &mut leader_out,
        );
        std::mem::swap(&mut leader_out.persist_ops, &mut leader_ops_buf);
        for op in leader_ops_buf.drain(..) {
            leader.step(Input::Persisted { token: op.token() }, &mut leader_out);
        }
        std::mem::swap(&mut leader_out.messages, &mut leader_msgs_buf);
        for msg in leader_msgs_buf.drain(..) {
            if msg.to == node2 {
                f2_out.clear();
                follower2.step(
                    Input::Recv {
                        from: node1,
                        message: msg.message,
                    },
                    &mut f2_out,
                );
                std::mem::swap(&mut f2_out.persist_ops, &mut f2_ops_buf);
                for op in f2_ops_buf.drain(..) {
                    follower2.step(Input::Persisted { token: op.token() }, &mut f2_out);
                }
                std::mem::swap(&mut f2_out.messages, &mut f2_msgs_buf);
                for resp in f2_msgs_buf.drain(..) {
                    leader.step(
                        Input::Recv {
                            from: node2,
                            message: resp.message,
                        },
                        &mut leader_out,
                    );
                }
            } else if msg.to == node3 {
                f3_out.clear();
                follower3.step(
                    Input::Recv {
                        from: node1,
                        message: msg.message,
                    },
                    &mut f3_out,
                );
                std::mem::swap(&mut f3_out.persist_ops, &mut f3_ops_buf);
                for op in f3_ops_buf.drain(..) {
                    follower3.step(Input::Persisted { token: op.token() }, &mut f3_out);
                }
                std::mem::swap(&mut f3_out.messages, &mut f3_msgs_buf);
                for resp in f3_msgs_buf.drain(..) {
                    leader.step(
                        Input::Recv {
                            from: node3,
                            message: resp.message,
                        },
                        &mut leader_out,
                    );
                }
            }
        }
    }

    let start = Instant::now();
    for _ in 0..ENTRIES {
        leader_out.clear();
        leader.step(
            Input::Propose {
                payload: payload.clone(),
            },
            &mut leader_out,
        );
        std::mem::swap(&mut leader_out.persist_ops, &mut leader_ops_buf);
        for op in leader_ops_buf.drain(..) {
            leader.step(Input::Persisted { token: op.token() }, &mut leader_out);
        }
        std::mem::swap(&mut leader_out.messages, &mut leader_msgs_buf);
        for msg in leader_msgs_buf.drain(..) {
            if msg.to == node2 {
                f2_out.clear();
                follower2.step(
                    Input::Recv {
                        from: node1,
                        message: msg.message,
                    },
                    &mut f2_out,
                );
                std::mem::swap(&mut f2_out.persist_ops, &mut f2_ops_buf);
                for op in f2_ops_buf.drain(..) {
                    follower2.step(Input::Persisted { token: op.token() }, &mut f2_out);
                }
                std::mem::swap(&mut f2_out.messages, &mut f2_msgs_buf);
                for resp in f2_msgs_buf.drain(..) {
                    leader.step(
                        Input::Recv {
                            from: node2,
                            message: resp.message,
                        },
                        &mut leader_out,
                    );
                }
            } else if msg.to == node3 {
                f3_out.clear();
                follower3.step(
                    Input::Recv {
                        from: node1,
                        message: msg.message,
                    },
                    &mut f3_out,
                );
                std::mem::swap(&mut f3_out.persist_ops, &mut f3_ops_buf);
                for op in f3_ops_buf.drain(..) {
                    follower3.step(Input::Persisted { token: op.token() }, &mut f3_out);
                }
                std::mem::swap(&mut f3_out.messages, &mut f3_msgs_buf);
                for resp in f3_msgs_buf.drain(..) {
                    leader.step(
                        Input::Recv {
                            from: node3,
                            message: resp.message,
                        },
                        &mut leader_out,
                    );
                }
            }
        }
    }
    let elapsed = start.elapsed();
    let entries_per_sec = ENTRIES as f64 / elapsed.as_secs_f64();
    (entries_per_sec, ENTRIES)
}

fn measure_wal_recovery_scan() -> (f64, usize) {
    use anvil_storage::{
        HardStateSlot, LogRecoveryClassification, SectorSize, StorageEngine, WalRecordFrame,
        WalSegmentHeader,
    };

    const RECORDS: usize = 10_000;
    const ITERATIONS: usize = 10;

    let sector_size = SectorSize::Sector512;
    let sector_len = sector_size.as_usize();

    // Prepare HardState buffer
    let mut hs_buf = vec![0u8; sector_len * 2];
    let hs_slot = HardStateSlot::new(
        0,
        1,
        Term::from_raw(1),
        Some(NodeId::new(1).expect("node 1")),
    );
    hs_slot
        .encode(sector_size, &mut hs_buf[..sector_len])
        .expect("encode hs");

    // Prepare WAL segment buffer: 10,000 records of 1 KiB each
    let header = WalSegmentHeader::new(sector_size, 1, LogIndex::from_raw(1), Term::from_raw(1));
    let mut wal_buf = Vec::with_capacity(sector_len + RECORDS * 1536);
    let mut hdr_bytes = vec![0u8; sector_len];
    header.encode(&mut hdr_bytes).expect("encode wal header");
    wal_buf.extend_from_slice(&hdr_bytes);

    let payload = Bytes::from(vec![0x42u8; 1024]);
    let mut prev_crc = header.header_crc;
    for i in 1..=RECORDS {
        let entry = Entry {
            index: LogIndex::from_raw(i as u64),
            term: Term::from_raw(1),
            kind: EntryKind::Normal,
            payload: payload.clone(),
        };
        let frame = WalRecordFrame::from_entry(&entry, prev_crc, 1, wal_buf.len() as u64);
        frame.encode_frame(sector_size, &mut wal_buf);
        prev_crc = frame.crc32c;
    }

    let wal_bytes = Bytes::from(wal_buf);

    // Warm-up
    let recovered =
        StorageEngine::scan_recovery(&hs_buf, &[(header.clone(), &wal_bytes)], sector_size)
            .expect("warmup scan recovery");
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::Clean
    ));

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let rec =
            StorageEngine::scan_recovery(&hs_buf, &[(header.clone(), &wal_bytes)], sector_size)
                .expect("scan recovery iteration");
        std::hint::black_box(&rec);
    }
    let elapsed = start.elapsed();

    let total_scanned_bytes = wal_bytes.len() * ITERATIONS;
    let gb_per_sec = (total_scanned_bytes as f64) / elapsed.as_secs_f64() / 1_000_000_000.0;
    (gb_per_sec, total_scanned_bytes)
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

    println!("Benchmarking Raft Replication (Leader + 2 Followers, 1 Core)...");
    let (raft_entries_sec, total_raft_entries) = measure_raft_replication();
    println!("  -> {raft_entries_sec:.0} entries/sec ({total_raft_entries} entries replicated)");

    println!("Benchmarking Storage WAL Recovery Scan (Real Codec + Chained CRC32C)...");
    let (wal_gb_sec, total_wal_bytes) = measure_wal_recovery_scan();
    println!("  -> {wal_gb_sec:.2} GB/sec ({total_wal_bytes} bytes scanned across 10 iterations)");

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
    println!(
        "| Raft Replication (entries/sec)  | {raft_entries_sec:>8.0}   | >= 50,000     | {budget_raft:>10.0}   |",
        budget_raft = raft_entries_sec * 0.8
    );
    println!(
        "| WAL Recovery Scan (GB/sec)      | {wal_gb_sec:>8.2} GB | >= 1.00 GB/s  | {budget_wal:>10.2} GB |",
        budget_wal = wal_gb_sec * 0.8
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
        if raft_entries_sec < 50_000.0 {
            eprintln!(
                "FAIL: Raft replication throughput {raft_entries_sec:.0} entries/sec < 50,000 floor"
            );
            passed = false;
        }
        if wal_gb_sec < 1.0 {
            eprintln!("FAIL: WAL recovery scan throughput {wal_gb_sec:.2} GB/s < 1.0 GB/s floor");
            passed = false;
        }
        if !passed {
            std::process::exit(1);
        }
    }
}
