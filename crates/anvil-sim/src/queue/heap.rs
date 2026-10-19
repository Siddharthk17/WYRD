use super::{EventQueue, ScheduledEvent};
use crate::time::SimTime;
use core::cmp::Reverse;
use std::collections::BinaryHeap;

/// Deterministic event queue backed by a 4-ary or standard binary min-heap.
#[derive(Debug, Default, Clone)]
pub struct BinaryHeapEventQueue {
    heap: BinaryHeap<Reverse<ScheduledEvent>>,
    next_seq: u64,
}

impl BinaryHeapEventQueue {
    /// Creates a new empty event queue.
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_seq: 0,
        }
    }

    /// Creates a new empty event queue with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity),
            next_seq: 0,
        }
    }

    /// Resets the queue state.
    pub fn clear(&mut self) {
        self.heap.clear();
        self.next_seq = 0;
    }
}

impl EventQueue for BinaryHeapEventQueue {
    fn push(&mut self, time: SimTime, actor_id: u64, payload: u64) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);

        let event = ScheduledEvent {
            time,
            insertion_seq: seq,
            actor_id,
            payload,
        };
        self.heap.push(Reverse(event));
        seq
    }

    fn pop(&mut self) -> Option<ScheduledEvent> {
        self.heap.pop().map(|Reverse(ev)| ev)
    }

    fn peek(&self) -> Option<&ScheduledEvent> {
        self.heap.peek().map(|Reverse(ev)| ev)
    }

    fn len(&self) -> usize {
        self.heap.len()
    }
}
