#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_precision_loss
)]

//! Counting allocator test verifying steady-state allocations for `StorageEngine`.

use anvil_storage::{EngineInput, EngineOutput, SectorSize, StorageConfig, StorageEngine};
use anvil_types::LogIndex;
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

#[test]
fn test_storage_engine_reused_output_clears_without_allocation() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap();

    let mut output = EngineOutput::default();
    output.device_ops.reserve(64);
    output.persisted_tokens.reserve(64);

    // Warm-up
    output
        .device_ops
        .push(anvil_storage::DeviceOp::SyncDir { op_id: 1 });
    output.persisted_tokens.push(100);
    output.clear();

    enable_tracking();
    output.clear();
    let allocs = disable_tracking();

    assert_eq!(
        allocs, 0,
        "EngineOutput::clear must perform zero allocations"
    );
}

#[test]
fn test_empty_append_zero_allocations() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap();

    let config = StorageConfig::for_test(SectorSize::Sector512, 1024 * 1024);
    let mut engine = StorageEngine::new_empty(config);
    let mut output = EngineOutput::default();
    output.device_ops.reserve(64);
    output.persisted_tokens.reserve(64);

    // Warm-up
    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(0),
            entries: Vec::new(),
            token: 1,
        },
        &mut output,
    );
    output.clear();

    enable_tracking();
    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(0),
            entries: Vec::new(),
            token: 2,
        },
        &mut output,
    );
    let allocs = disable_tracking();

    assert_eq!(allocs, 0, "Empty append must perform 0 allocations");
}
