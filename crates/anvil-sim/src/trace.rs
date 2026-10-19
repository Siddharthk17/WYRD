use crate::time::SimTime;

/// Compact 32-byte binary trace event recorded during simulation execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct TraceEvent {
    /// Virtual timestamp at event emission.
    pub time: SimTime,
    /// Actor or component that emitted the trace event.
    pub actor_id: u64,
    /// Event category or protocol state discriminant.
    pub event_kind: u16,
    /// Fixed-size payload bytes.
    pub payload: [u8; 14],
}

impl TraceEvent {
    /// Serializes the trace event into a 32-byte binary representation.
    pub fn to_bytes(self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&self.time.as_nanos().to_le_bytes());
        buf[8..16].copy_from_slice(&self.actor_id.to_le_bytes());
        buf[16..18].copy_from_slice(&self.event_kind.to_le_bytes());
        buf[18..32].copy_from_slice(&self.payload);
        buf
    }

    /// Deserializes a 32-byte buffer into a `TraceEvent`.
    pub fn from_bytes(buf: &[u8; 32]) -> Self {
        let time = SimTime::from_nanos(u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]));
        let actor_id = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        let event_kind = u16::from_le_bytes([buf[16], buf[17]]);
        let mut payload = [0u8; 14];
        payload.copy_from_slice(&buf[18..32]);

        Self {
            time,
            actor_id,
            event_kind,
            payload,
        }
    }
}

/// Trace recorder holding a ring buffer of recent events and a streaming BLAKE3 digest of all events.
#[derive(Debug)]
pub struct TraceRecorder {
    ring_buffer: Vec<TraceEvent>,
    capacity: usize,
    write_idx: usize,
    total_events: u64,
    hasher: blake3::Hasher,
}

impl TraceRecorder {
    /// Default ring buffer capacity (1024 events = 32 KiB memory).
    pub const DEFAULT_CAPACITY: usize = 1024;

    /// Creates a new `TraceRecorder` with the default capacity.
    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    /// Creates a new `TraceRecorder` with the specified ring buffer capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "trace ring buffer capacity must be > 0");
        Self {
            ring_buffer: Vec::with_capacity(capacity),
            capacity,
            write_idx: 0,
            total_events: 0,
            hasher: blake3::Hasher::new(),
        }
    }

    /// Records an event, updating the streaming BLAKE3 hash and appending to the ring buffer.
    pub fn record(&mut self, event: TraceEvent) {
        let event_bytes = event.to_bytes();
        self.hasher.update(&event_bytes);

        if self.ring_buffer.len() < self.capacity {
            self.ring_buffer.push(event);
        } else {
            self.ring_buffer[self.write_idx] = event;
        }

        self.write_idx = (self.write_idx + 1) % self.capacity;
        self.total_events = self.total_events.wrapping_add(1);
    }

    /// Returns the total number of events recorded since initialization.
    pub fn total_events(&self) -> u64 {
        self.total_events
    }

    /// Returns the contents of the ring buffer ordered from oldest to newest.
    pub fn recent_events(&self) -> Vec<TraceEvent> {
        let len = self.ring_buffer.len();
        if len < self.capacity {
            return self.ring_buffer.clone();
        }

        let mut result = Vec::with_capacity(len);
        for i in 0..len {
            let idx = (self.write_idx + i) % len;
            result.push(self.ring_buffer[idx]);
        }
        result
    }

    /// Finalizes and returns the 32-byte streaming BLAKE3 hash of all recorded events.
    pub fn finalize_hash(&self) -> blake3::Hash {
        self.hasher.finalize()
    }
}

impl Default for TraceRecorder {
    fn default() -> Self {
        Self::new()
    }
}
