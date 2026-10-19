pub mod heap;
pub mod wheel;

use crate::time::SimTime;

/// An event scheduled in the simulation event queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledEvent {
    /// Scheduled virtual time of arrival.
    pub time: SimTime,
    /// Monotonically increasing sequence number ensuring deterministic tie-breaking.
    pub insertion_seq: u64,
    /// Target actor identifier.
    pub actor_id: u64,
    /// Event payload or discriminant word.
    pub payload: u64,
}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        // Natural ordering is min-heap: earlier time first, then lower insertion sequence.
        self.time
            .cmp(&other.time)
            .then_with(|| self.insertion_seq.cmp(&other.insertion_seq))
    }
}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Abstract contract for simulation event queues.
pub trait EventQueue {
    /// Pushes a new event scheduled for `time` directed to `actor_id`.
    fn push(&mut self, time: SimTime, actor_id: u64, payload: u64) -> u64;

    /// Pops the next earliest event from the queue.
    fn pop(&mut self) -> Option<ScheduledEvent>;

    /// Peeks at the next earliest event without removing it.
    fn peek(&self) -> Option<&ScheduledEvent>;

    /// Returns the number of events currently enqueued.
    fn len(&self) -> usize;

    /// Returns true if no events are enqueued.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub use heap::BinaryHeapEventQueue;
pub use wheel::HierarchicalTimingWheel;
