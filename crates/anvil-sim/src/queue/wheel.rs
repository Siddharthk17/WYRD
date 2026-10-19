use super::{EventQueue, ScheduledEvent};
use crate::time::SimTime;

const WHEEL_BITS: usize = 6;
const WHEEL_SIZE: usize = 1 << WHEEL_BITS; // 64 slots
const WHEEL_MASK: u64 = (WHEEL_SIZE as u64) - 1;
const NUM_LEVELS: usize = 4;

/// Hierarchical Timing Wheel implementation with 4 levels (64 slots per level) and an overflow list.
#[derive(Debug, Clone)]
pub struct HierarchicalTimingWheel {
    levels: [Vec<Vec<ScheduledEvent>>; NUM_LEVELS],
    overflow: Vec<ScheduledEvent>,
    current_time: SimTime,
    next_seq: u64,
    count: usize,
}

impl Default for HierarchicalTimingWheel {
    fn default() -> Self {
        Self::new()
    }
}

impl HierarchicalTimingWheel {
    /// Creates a new empty hierarchical timing wheel.
    pub fn new() -> Self {
        let levels = std::array::from_fn(|_| {
            let mut v = Vec::with_capacity(WHEEL_SIZE);
            for _ in 0..WHEEL_SIZE {
                v.push(Vec::new());
            }
            v
        });

        Self {
            levels,
            overflow: Vec::new(),
            current_time: SimTime::ZERO,
            next_seq: 0,
            count: 0,
        }
    }

    fn insert_event(&mut self, event: ScheduledEvent) {
        let delta = event.time.saturating_duration_since(self.current_time);

        if delta < WHEEL_SIZE as u64 {
            let slot = ((self.current_time.as_nanos() + delta) & WHEEL_MASK) as usize;
            self.levels[0][slot].push(event);
        } else if delta < (1 << (2 * WHEEL_BITS)) {
            let slot =
                (((self.current_time.as_nanos() + delta) >> WHEEL_BITS) & WHEEL_MASK) as usize;
            self.levels[1][slot].push(event);
        } else if delta < (1 << (3 * WHEEL_BITS)) {
            let slot = (((self.current_time.as_nanos() + delta) >> (2 * WHEEL_BITS)) & WHEEL_MASK)
                as usize;
            self.levels[2][slot].push(event);
        } else if delta < (1 << (4 * WHEEL_BITS)) {
            let slot = (((self.current_time.as_nanos() + delta) >> (3 * WHEEL_BITS)) & WHEEL_MASK)
                as usize;
            self.levels[3][slot].push(event);
        } else {
            self.overflow.push(event);
        }
    }

    fn cascade_level(&mut self, level: usize) {
        let shift = level * WHEEL_BITS;
        let slot = ((self.current_time.as_nanos() >> shift) & WHEEL_MASK) as usize;
        let events = std::mem::take(&mut self.levels[level][slot]);
        for ev in events {
            self.insert_event(ev);
        }
    }
}

impl EventQueue for HierarchicalTimingWheel {
    fn push(&mut self, time: SimTime, actor_id: u64, payload: u64) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);

        let event = ScheduledEvent {
            time,
            insertion_seq: seq,
            actor_id,
            payload,
        };

        self.insert_event(event);
        self.count += 1;
        seq
    }

    fn pop(&mut self) -> Option<ScheduledEvent> {
        if self.count == 0 {
            return None;
        }

        // Search for earliest event across current slot and levels
        loop {
            let slot0 = (self.current_time.as_nanos() & WHEEL_MASK) as usize;
            if !self.levels[0][slot0].is_empty() {
                // Find earliest event in slot by (time, insertion_seq)
                let bucket = &mut self.levels[0][slot0];
                let mut best_idx = 0;
                for i in 1..bucket.len() {
                    if bucket[i] < bucket[best_idx] {
                        best_idx = i;
                    }
                }
                let event = bucket.swap_remove(best_idx);
                self.count -= 1;
                self.current_time = event.time;
                return Some(event);
            }

            // Need to advance or cascade
            // Check if all levels are empty except overflow
            let mut found_next_time = None;

            for level in &self.levels {
                for slot in level {
                    for ev in slot {
                        match found_next_time {
                            None => found_next_time = Some(ev.time),
                            Some(t) if ev.time < t => found_next_time = Some(ev.time),
                            _ => {}
                        }
                    }
                }
            }

            for ev in &self.overflow {
                match found_next_time {
                    None => found_next_time = Some(ev.time),
                    Some(t) if ev.time < t => found_next_time = Some(ev.time),
                    _ => {}
                }
            }

            let next_time = found_next_time?;
            self.current_time = next_time;
            // Cascade higher levels
            for lvl in (1..NUM_LEVELS).rev() {
                self.cascade_level(lvl);
            }
            // Re-insert overflow items that now fit in wheels
            let overflow_items = std::mem::take(&mut self.overflow);
            for ev in overflow_items {
                self.insert_event(ev);
            }
        }
    }

    fn peek(&self) -> Option<&ScheduledEvent> {
        if self.count == 0 {
            return None;
        }

        let mut best: Option<&ScheduledEvent> = None;
        for level in &self.levels {
            for slot in level {
                for ev in slot {
                    match best {
                        None => best = Some(ev),
                        Some(b) if ev < b => best = Some(ev),
                        _ => {}
                    }
                }
            }
        }
        for ev in &self.overflow {
            match best {
                None => best = Some(ev),
                Some(b) if ev < b => best = Some(ev),
                _ => {}
            }
        }
        best
    }

    fn len(&self) -> usize {
        self.count
    }
}
