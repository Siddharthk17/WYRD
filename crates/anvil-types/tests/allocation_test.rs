#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Performance regression test asserting zero-allocation hot paths using a counting global allocator.
//!
//! Specifically validates the Charter requirement:
//! "codec decode of 1 KiB entries with zero allocations".

use anvil_types::{Entry, EntryKind, Frame, FrameType, LogIndex, Term};
use bytes::Bytes;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAlloc;

thread_local! {
    static TRACKING_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static THREAD_ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    static THREAD_ALLOC_BYTES: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = TRACKING_ACTIVE.try_with(|active| {
            if active.get() {
                let _ = THREAD_ALLOC_COUNT.try_with(|c| c.set(c.get() + 1));
                let _ = THREAD_ALLOC_BYTES.try_with(|b| b.set(b.get() + layout.size()));
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

static ALLOC_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn enable_tracking() {
    let _ = THREAD_ALLOC_COUNT.try_with(|c| c.set(0));
    let _ = THREAD_ALLOC_BYTES.try_with(|b| b.set(0));
    let _ = TRACKING_ACTIVE.try_with(|a| a.set(true));
}

fn disable_tracking() -> usize {
    let _ = TRACKING_ACTIVE.try_with(|a| a.set(false));
    THREAD_ALLOC_COUNT.try_with(Cell::get).unwrap_or(0)
}

fn current_alloc_count() -> usize {
    THREAD_ALLOC_COUNT.try_with(Cell::get).unwrap_or(0)
}

#[test]
fn test_allocator_tracks_allocations() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap();
    enable_tracking();
    assert_eq!(current_alloc_count(), 0);

    let boxed = Box::new(42u64);
    assert_eq!(*boxed, 42);
    let count = disable_tracking();
    assert!(count > 0, "CountingAlloc must record heap allocations");
}

#[test]
fn test_codec_decode_1kib_entry_zero_allocations() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap();
    let payload_1k = vec![0xFEu8; 1024];
    let entry = Entry {
        index: LogIndex::from_raw(100),
        term: Term::from_raw(4),
        kind: EntryKind::Normal,
        payload: Bytes::from(payload_1k),
    };

    let mut single_encoded = Vec::new();
    entry.encode(&mut single_encoded);

    // Stream buffer containing two 1 KiB entries
    let mut stream_buf = Vec::new();
    stream_buf.extend_from_slice(&single_encoded);
    stream_buf.extend_from_slice(&single_encoded);

    let mut stream_bytes = Bytes::from(stream_buf);

    // Initial decode establishes steady-state shared buffer reference
    let _warmup = Entry::decode_from_bytes(&mut stream_bytes).expect("warmup decode");

    // Reset allocation counter before testing hot path steady-state
    enable_tracking();

    // Hot path steady-state: zero-copy decode of 1 KiB entry
    let decoded = Entry::decode_from_bytes(&mut stream_bytes).expect("hot path decode");

    let allocs = disable_tracking();
    assert_eq!(
        allocs, 0,
        "codec decode of 1 KiB entry in steady state must incur exactly ZERO allocations! Observed: {allocs}"
    );

    assert_eq!(decoded.index.as_u64(), 100);
    assert_eq!(decoded.term.as_u64(), 4);
    assert_eq!(decoded.kind, EntryKind::Normal);
    assert_eq!(decoded.payload.len(), 1024);
    assert_eq!(decoded.payload[0], 0xFE);
}

#[test]
fn test_codec_decode_batch_1000_entries_zero_allocations() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap();
    let payload_1k = vec![0xABu8; 1024];
    let entry = Entry {
        index: LogIndex::from_raw(1),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from(payload_1k),
    };

    let mut single_encoded = Vec::new();
    entry.encode(&mut single_encoded);

    let mut stream_buf = Vec::with_capacity(single_encoded.len() * 1001);
    for _ in 0..1001 {
        stream_buf.extend_from_slice(&single_encoded);
    }

    let mut stream_bytes = Bytes::from(stream_buf);

    // Warmup: first decode transitions buffer to steady-state shared reference
    let _warmup = Entry::decode_from_bytes(&mut stream_bytes).expect("warmup decode");

    enable_tracking();

    // Decode 1,000 entries sequentially from the wire buffer in steady state
    let mut decoded_count = 0;
    while !stream_bytes.is_empty() {
        let entry = Entry::decode_from_bytes(&mut stream_bytes).expect("stream decode");
        assert_eq!(entry.payload.len(), 1024);
        decoded_count += 1;
    }

    let allocs = disable_tracking();
    assert_eq!(decoded_count, 1000);
    assert_eq!(
        allocs, 0,
        "batch decode of 1,000 1-KiB entries in steady state must incur ZERO allocations! Observed: {allocs}"
    );
}

#[test]
fn test_frame_decode_zero_allocations() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap();
    let payload = vec![0x42u8; 1024];
    let mut single_frame = Vec::new();
    Frame::encode(FrameType::Entry, &payload, &mut single_frame);

    // Stream containing two frames
    let mut stream_buf = Vec::new();
    stream_buf.extend_from_slice(&single_frame);
    stream_buf.extend_from_slice(&single_frame);

    let mut frame_bytes = Bytes::from(stream_buf);

    // Warmup decode promotes buffer to steady-state shared reference
    let _warmup = Frame::decode_from_bytes(&mut frame_bytes).expect("warmup frame decode");

    enable_tracking();

    // Steady state hot path decode
    let decoded_frame = Frame::decode_from_bytes(&mut frame_bytes).expect("decode frame");
    let allocs = disable_tracking();

    assert_eq!(
        allocs, 0,
        "frame decode of 1 KiB payload in steady state must incur ZERO allocations! Observed: {allocs}"
    );
    assert_eq!(decoded_frame.payload.len(), 1024);
    assert_eq!(decoded_frame.header.payload_len, 1024);
}
