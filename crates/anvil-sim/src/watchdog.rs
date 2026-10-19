use crate::time::SimTime;
use thiserror::Error;

/// Watchdog error indicating simulation protocol stall or premature termination.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum WatchdogError {
    #[error(
        "simulation queue emptied prematurely at {current_time} (expected at least {expected_min_time})"
    )]
    PrematureDepletion {
        current_time: SimTime,
        expected_min_time: SimTime,
    },

    #[error(
        "simulation stalled: {events_without_progress} events elapsed without a progress marker (limit: {limit})"
    )]
    StagnationDetected {
        events_without_progress: u64,
        limit: u64,
    },
}

/// Watchdog tracking progress markers and preventing infinite loops or premature termination.
#[derive(Debug, Clone)]
pub struct Watchdog {
    expected_min_time: Option<SimTime>,
    max_stagnant_events: u64,
    events_without_progress: u64,
}

impl Watchdog {
    /// Default limit of events permitted without progress before triggering stall detection.
    pub const DEFAULT_MAX_STAGNANT_EVENTS: u64 = 10_000;

    /// Creates a new watchdog with the specified stall limit and optional minimum duration.
    pub fn new(expected_min_time: Option<SimTime>, max_stagnant_events: u64) -> Self {
        Self {
            expected_min_time,
            max_stagnant_events,
            events_without_progress: 0,
        }
    }

    /// Records progress (e.g. committed log entry, state machine transition, heartbeat received),
    /// resetting the stagnation counter.
    pub fn record_progress(&mut self) {
        self.events_without_progress = 0;
    }

    /// Invoked upon processing every simulation event to check for protocol stagnation.
    pub fn on_event(&mut self) -> Result<(), WatchdogError> {
        self.events_without_progress = self.events_without_progress.saturating_add(1);
        if self.events_without_progress >= self.max_stagnant_events {
            return Err(WatchdogError::StagnationDetected {
                events_without_progress: self.events_without_progress,
                limit: self.max_stagnant_events,
            });
        }
        Ok(())
    }

    /// Invoked when the simulation event queue becomes empty to check for premature termination.
    pub fn on_queue_empty(&self, now: SimTime) -> Result<(), WatchdogError> {
        if let Some(min_time) = self.expected_min_time
            && now < min_time
        {
            return Err(WatchdogError::PrematureDepletion {
                current_time: now,
                expected_min_time: min_time,
            });
        }
        Ok(())
    }

    /// Returns the number of events processed without progress markers.
    pub fn events_without_progress(&self) -> u64 {
        self.events_without_progress
    }
}

impl Default for Watchdog {
    fn default() -> Self {
        Self::new(None, Self::DEFAULT_MAX_STAGNANT_EVENTS)
    }
}
