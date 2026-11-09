use crate::buggify::{BuggifyPoint, list_registered_points};
use crate::queue::{BinaryHeapEventQueue, EventQueue, ScheduledEvent};
use crate::rng::DetRng;
use crate::time::SimTime;
use crate::trace::{TraceEvent, TraceRecorder};
use crate::watchdog::{Watchdog, WatchdogError};
use std::collections::BTreeMap;
use thiserror::Error;

/// Error returned during simulation kernel execution.
#[derive(Debug, Error)]
pub enum SimError {
    /// Watchdog fired: queue emptied early or no progress marker observed.
    #[error("watchdog triggered: {0}")]
    Watchdog(#[from] WatchdogError),

    /// Event targeted an actor id with no registered actor.
    #[error("unknown target actor: {0}")]
    UnknownActor(u64),

    /// Event timestamp moved backward. Times never move backward.
    #[error(
        "simulation time regression: current time {current} advanced to earlier time {attempted}"
    )]
    TimeRegression {
        /// Time already reached by the kernel.
        current: SimTime,
        /// Earlier time the popped event carried.
        attempted: SimTime,
    },
}

/// Simulation outcome report containing execution statistics and cryptographic trace hash.
#[derive(Debug, Clone)]
pub struct SimReport {
    /// Virtual time of the last processed event.
    pub final_time: SimTime,
    /// Total events dispatched.
    pub events_processed: u64,
    /// Streaming BLAKE3 digest of the full event trace.
    pub trace_hash: blake3::Hash,
    /// Buggify points evaluated with hit counts.
    pub buggify_points: Vec<BuggifyPoint>,
}

/// Context passed to actors during event dispatch.
pub struct ActorContext<'a> {
    now: SimTime,
    out: &'a mut Vec<(SimTime, u64, u64)>,
    progress: &'a mut bool,
}

impl<'a> ActorContext<'a> {
    /// Creates a new actor context.
    pub fn new(
        now: SimTime,
        out: &'a mut Vec<(SimTime, u64, u64)>,
        progress: &'a mut bool,
    ) -> Self {
        Self { now, out, progress }
    }

    /// Returns the current simulation virtual time.
    #[inline]
    pub fn now(&self) -> SimTime {
        self.now
    }

    /// Schedules an outbound event for an actor at the given simulation time.
    #[inline]
    pub fn schedule(&mut self, time: SimTime, target_actor: u64, payload: u64) {
        self.out.push((time, target_actor, payload));
    }

    /// Signals forward protocol progress to the watchdog, resetting the stagnation counter.
    #[inline]
    pub fn record_progress(&mut self) {
        *self.progress = true;
    }
}

/// Abstract contract for actors participating in the deterministic simulation.
pub trait Actor: 'static {
    /// Dispatches an event arrival to the actor.
    fn on_event(&mut self, ctx: &mut ActorContext<'_>, payload: u64);

    /// Downcasting support for inspection.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Mutable downcasting support for inspection.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Deterministic simulation kernel orchestrating actor execution, time advancement, and trace logging.
pub struct SimKernel {
    queue: BinaryHeapEventQueue,
    actors: BTreeMap<u64, Box<dyn Actor>>,
    now: SimTime,
    events_processed: u64,
    max_virtual_time: Option<SimTime>,
    max_events: Option<u64>,
    watchdog: Watchdog,
    tracer: TraceRecorder,
    rng: DetRng,
    scratch: Vec<(SimTime, u64, u64)>,
}

impl SimKernel {
    /// Constructs a new simulation kernel with the given master seed.
    pub fn new(seed: u64) -> Self {
        Self {
            queue: BinaryHeapEventQueue::new(),
            actors: BTreeMap::new(),
            now: SimTime::ZERO,
            events_processed: 0,
            max_virtual_time: None,
            max_events: None,
            watchdog: Watchdog::default(),
            tracer: TraceRecorder::new(),
            rng: DetRng::new(seed, "kernel"),
            scratch: Vec::new(),
        }
    }

    /// Sets the maximum virtual time limit.
    #[must_use]
    pub fn with_max_virtual_time(mut self, limit: SimTime) -> Self {
        self.max_virtual_time = Some(limit);
        self
    }

    /// Sets the maximum total event limit.
    #[must_use]
    pub fn with_max_events(mut self, limit: u64) -> Self {
        self.max_events = Some(limit);
        self
    }

    /// Configures the simulation watchdog.
    #[must_use]
    pub fn with_watchdog(mut self, watchdog: Watchdog) -> Self {
        self.watchdog = watchdog;
        self
    }

    /// Registers an actor in the kernel registry.
    pub fn register_actor(&mut self, actor_id: u64, actor: Box<dyn Actor>) {
        self.actors.insert(actor_id, actor);
    }

    /// Returns an immutable reference to an actor downcasted to concrete type `T`.
    pub fn actor<T: 'static>(&self, actor_id: u64) -> Option<&T> {
        self.actors
            .get(&actor_id)
            .and_then(|listed| listed.as_any().downcast_ref::<T>())
    }

    /// Returns a mutable reference to an actor downcasted to concrete type `T`.
    pub fn actor_mut<T: 'static>(&mut self, actor_id: u64) -> Option<&mut T> {
        self.actors
            .get_mut(&actor_id)
            .and_then(|listed| listed.as_any_mut().downcast_mut::<T>())
    }

    /// Schedules an event in the simulation event queue.
    pub fn schedule(&mut self, time: SimTime, actor_id: u64, payload: u64) -> u64 {
        self.queue.push(time, actor_id, payload)
    }

    /// Records progress, signaling to the watchdog that the system is not stalled.
    pub fn record_progress(&mut self) {
        self.watchdog.record_progress();
    }

    /// Returns the current simulation virtual time.
    pub fn now(&self) -> SimTime {
        self.now
    }

    /// Returns the total events processed so far.
    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    /// Returns a mutable reference to the kernel's deterministic random generator.
    pub fn rng(&mut self) -> &mut DetRng {
        &mut self.rng
    }

    /// Returns a reference to the trace recorder.
    pub fn tracer(&self) -> &TraceRecorder {
        &self.tracer
    }

    /// Delivers one event payload to its actor, collecting outbound events into `out`.
    fn dispatch_to_actor(
        &mut self,
        actor_id: u64,
        payload: u64,
        out: &mut Vec<(SimTime, u64, u64)>,
    ) -> Result<(), SimError> {
        let now = self.now;
        let Some(actor) = self.actors.get_mut(&actor_id) else {
            return Err(SimError::UnknownActor(actor_id));
        };
        let mut progress_made = false;
        let mut ctx = ActorContext::new(now, out, &mut progress_made);
        actor.on_event(&mut ctx, payload);
        if progress_made {
            self.watchdog.record_progress();
        }
        Ok(())
    }

    /// Steps the simulation by exactly one event.
    pub fn step(&mut self) -> Result<Option<ScheduledEvent>, SimError> {
        if let Some(max_time) = self.max_virtual_time
            && self.now >= max_time
        {
            return Ok(None);
        }
        if let Some(max_ev) = self.max_events
            && self.events_processed >= max_ev
        {
            return Ok(None);
        }

        let Some(event) = self.queue.pop() else {
            self.watchdog.on_queue_empty(self.now)?;
            return Ok(None);
        };

        if event.time < self.now {
            return Err(SimError::TimeRegression {
                current: self.now,
                attempted: event.time,
            });
        }

        self.now = event.time;
        self.events_processed = self.events_processed.saturating_add(1);
        self.watchdog.on_event()?;

        let mut trace_payload = [0u8; 14];
        trace_payload[0..8].copy_from_slice(&event.payload.to_le_bytes());
        self.tracer.record(TraceEvent {
            time: self.now,
            actor_id: event.actor_id,
            event_kind: 1,
            payload: trace_payload,
        });

        let mut out_events = std::mem::take(&mut self.scratch);
        let dispatch = self.dispatch_to_actor(event.actor_id, event.payload, &mut out_events);
        for (target_time, target_actor, payload) in out_events.drain(..) {
            self.schedule(target_time, target_actor, payload);
        }
        self.scratch = out_events;
        dispatch?;

        Ok(Some(event))
    }

    /// Executes the simulation until completion or cap exhaustion.
    pub fn run(&mut self) -> Result<SimReport, SimError> {
        while let Some(_event) = self.step()? {}

        Ok(SimReport {
            final_time: self.now,
            events_processed: self.events_processed,
            trace_hash: self.tracer.finalize_hash(),
            buggify_points: list_registered_points(),
        })
    }
}
