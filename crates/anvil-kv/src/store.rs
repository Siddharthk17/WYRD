#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

//! Deterministic KV store over `imbl::OrdMap` with session deduplication.

use crate::command::{KvCommand, KvCommandError, KvResponse};
use crate::snapshot::{
    ChunkError, SnapshotChunk, reassemble_chunks, snapshot_digest, split_into_chunks,
};
use anvil_types::{LogIndex, MembershipSnapshot, SeqNo, SessionId};
use bytes::Bytes;
use std::collections::BTreeMap;
use thiserror::Error;

/// Limits and session policy for the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvConfig {
    /// Maximum key length in bytes.
    pub max_key_bytes: usize,
    /// Maximum value length in bytes.
    pub max_value_bytes: usize,
    /// Virtual ticks of idleness before a session expires.
    pub session_idle_ticks: u64,
    /// Maximum live sessions held in memory.
    pub max_sessions: usize,
    /// Maximum cached responses retained per session.
    pub max_responses_per_session: usize,
}

impl Default for KvConfig {
    fn default() -> Self {
        Self {
            max_key_bytes: 1024,
            max_value_bytes: 1024 * 1024,
            session_idle_ticks: 10_000,
            max_sessions: 4096,
            max_responses_per_session: 512,
        }
    }
}

/// Errors from store operations. Bounds are checked before reserving memory.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum KvError {
    #[error("key too large: {0} bytes")]
    KeyTooLarge(usize),
    #[error("value too large: {0} bytes")]
    ValueTooLarge(usize),
    #[error("command decode failed")]
    Decode,
    #[error("too many sessions")]
    TooManySessions,
    #[error("snapshot chunk corrupt")]
    CorruptChunk,
}

impl From<KvCommandError> for KvError {
    fn from(_: KvCommandError) -> Self {
        Self::Decode
    }
}

/// One key's current value. Register keys hold a single element, list keys
/// hold appended elements in apply order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRecord {
    /// `true` for list-append keys, `false` for register keys.
    pub is_list: bool,
    /// Current elements. Register keys hold at most one.
    pub elements: Vec<Bytes>,
    /// Log index of the last write to this key.
    pub version: LogIndex,
}

/// Session identifier view used in snapshot headers.
pub type SessionIdView = SessionId;

/// Per-session deduplication and liveness state.
#[derive(Debug, Clone)]
struct SessionState {
    last_activity: u64,
    responses: BTreeMap<SeqNo, KvResponse>,
    highest_seq: u64,
}

/// Deterministic state machine. The persistent map is an `imbl::OrdMap` so a
/// snapshot is an `O(1)` clone. Time advances only through `ClockTick`.
#[derive(Debug, Clone)]
pub struct KvStore {
    config: KvConfig,
    map: imbl::OrdMap<Bytes, KvRecord>,
    sessions: BTreeMap<SessionId, SessionState>,
    session_order: BTreeMap<SessionId, u64>,
    virtual_tick: u64,
    state_hash: [u8; 32],
    applied_index: LogIndex,
    /// Versions of deleted keys. A delete is a write, so a read of a
    /// deleted key reports the delete index instead of zero.
    tombstones: BTreeMap<Bytes, LogIndex>,
}

impl KvStore {
    /// Creates an empty store with the given limits.
    #[must_use]
    pub fn new(config: KvConfig) -> Self {
        Self {
            config,
            map: imbl::OrdMap::new(),
            sessions: BTreeMap::new(),
            session_order: BTreeMap::new(),
            virtual_tick: 0,
            state_hash: [0u8; 32],
            applied_index: LogIndex::from_raw(0),
            tombstones: BTreeMap::new(),
        }
    }

    /// Returns the store limits.
    #[must_use]
    pub const fn config(&self) -> KvConfig {
        self.config
    }

    /// Returns the current virtual tick.
    #[must_use]
    pub const fn virtual_tick(&self) -> u64 {
        self.virtual_tick
    }

    /// Returns the rolling state hash over applied commands.
    #[must_use]
    pub const fn state_hash(&self) -> [u8; 32] {
        self.state_hash
    }

    /// Returns the highest applied log index.
    #[must_use]
    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    /// Returns the number of keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns true when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Applies one committed log entry. `RegisterSession` binds the session id
    /// to the entry index, which keeps session creation deterministic across
    /// replicas.
    pub fn apply(&mut self, index: LogIndex, payload: &[u8]) -> Result<KvResponse, KvError> {
        let cmd = KvCommand::decode(
            payload,
            self.config.max_key_bytes,
            self.config.max_value_bytes,
        )?;
        let response = self.apply_command(index, cmd)?;
        self.applied_index = index;
        Ok(response)
    }

    #[allow(clippy::too_many_lines)]
    fn apply_command(&mut self, index: LogIndex, cmd: KvCommand) -> Result<KvResponse, KvError> {
        match cmd {
            KvCommand::ClockTick { tick } => {
                if tick > self.virtual_tick {
                    self.virtual_tick = tick;
                }
                self.mix_hash(index, 6, &[], &[]);
                Ok(KvResponse::Ok)
            }
            KvCommand::ExpireSessions { up_to_tick } => {
                let removed = self.expire_sessions(up_to_tick);
                self.mix_hash(index, 7, &[], &up_to_tick.to_le_bytes());
                Ok(KvResponse::SessionsExpired { removed })
            }
            KvCommand::RegisterSession => {
                let session = SessionId::from_raw(index.as_u64());
                self.register_session(session)?;
                self.mix_hash(index, 1, &[], &index.as_u64().to_le_bytes());
                Ok(KvResponse::SessionRegistered { session })
            }
            KvCommand::Put {
                session,
                seq,
                acked,
                key,
                value,
            } => {
                if let Some(cached) = self.dedup_lookup(session, seq, acked) {
                    return Ok(cached);
                }
                self.check_bounds(&key, &value)?;
                self.touch_session(session, seq)?;
                self.tombstones.remove(&key);
                self.map.insert(
                    key.clone(),
                    KvRecord {
                        is_list: false,
                        elements: vec![value.clone()],
                        version: index,
                    },
                );
                self.mix_hash(index, 2, &key, &value);
                let response = KvResponse::Value {
                    value: Some(value),
                    version: index,
                };
                self.memoize(session, seq, &response);
                Ok(response)
            }
            KvCommand::Delete {
                session,
                seq,
                acked,
                key,
            } => {
                if let Some(cached) = self.dedup_lookup(session, seq, acked) {
                    return Ok(cached);
                }
                self.check_bounds(&key, &[])?;
                self.touch_session(session, seq)?;
                self.map.remove(&key);
                self.tombstones.insert(key.clone(), index);
                self.mix_hash(index, 3, &key, &[]);
                let response = KvResponse::Value {
                    value: None,
                    version: index,
                };
                self.memoize(session, seq, &response);
                Ok(response)
            }
            KvCommand::Cas {
                session,
                seq,
                acked,
                key,
                expected_version,
                value,
            } => {
                if let Some(cached) = self.dedup_lookup(session, seq, acked) {
                    return Ok(cached);
                }
                self.check_bounds(&key, &value)?;
                self.touch_session(session, seq)?;
                let current = self
                    .map
                    .get(&key)
                    .map_or(LogIndex::from_raw(0), |r| r.version);
                let current = if self.map.contains_key(&key) {
                    current
                } else {
                    self.tombstones.get(&key).copied().unwrap_or(current)
                };
                if current != expected_version {
                    let response = KvResponse::CasConflict { version: current };
                    self.memoize(session, seq, &response);
                    return Ok(response);
                }
                self.tombstones.remove(&key);
                self.map.insert(
                    key.clone(),
                    KvRecord {
                        is_list: false,
                        elements: vec![value.clone()],
                        version: index,
                    },
                );
                self.mix_hash(index, 4, &key, &value);
                let response = KvResponse::Value {
                    value: Some(value),
                    version: index,
                };
                self.memoize(session, seq, &response);
                Ok(response)
            }
            KvCommand::Append {
                session,
                seq,
                acked,
                key,
                value,
            } => {
                if let Some(cached) = self.dedup_lookup(session, seq, acked) {
                    return Ok(cached);
                }
                self.check_bounds(&key, &value)?;
                self.touch_session(session, seq)?;
                self.tombstones.remove(&key);
                match self.map.get(&key).cloned() {
                    Some(mut record) => {
                        record.elements.push(value.clone());
                        record.version = index;
                        record.is_list = true;
                        self.map.insert(key.clone(), record);
                    }
                    None => {
                        self.map.insert(
                            key.clone(),
                            KvRecord {
                                is_list: true,
                                elements: vec![value.clone()],
                                version: index,
                            },
                        );
                    }
                }
                self.mix_hash(index, 5, &key, &value);
                let record = self
                    .map
                    .get(&key)
                    .map_or(Vec::new(), |r| r.elements.clone());
                let response = KvResponse::List {
                    values: record,
                    version: index,
                };
                self.memoize(session, seq, &response);
                Ok(response)
            }
        }
    }

    /// Linearizable register read evaluated at the caller's read index.
    #[must_use]
    pub fn read_register(&self, key: &[u8]) -> KvResponse {
        match self.map.get(key) {
            Some(record) => KvResponse::Value {
                value: record.elements.last().cloned(),
                version: record.version,
            },
            None => KvResponse::Value {
                value: None,
                version: self.tombstones.get(key).copied().unwrap_or_default(),
            },
        }
    }

    /// Linearizable list read evaluated at the caller's read index.
    #[must_use]
    pub fn read_list(&self, key: &[u8]) -> KvResponse {
        match self.map.get(key) {
            Some(record) => KvResponse::List {
                values: record.elements.clone(),
                version: record.version,
            },
            None => KvResponse::List {
                values: Vec::new(),
                version: self.tombstones.get(key).copied().unwrap_or_default(),
            },
        }
    }

    /// Bounded whole-store range scan. Reads never enter the log.
    pub fn read_range(&self, start: &[u8], end: &[u8], limit: usize) -> KvResponse {
        let mut pairs = Vec::new();
        for (key, record) in &self.map {
            if key.as_ref() >= start && (end.is_empty() || key.as_ref() < end) {
                if let Some(value) = record.elements.last() {
                    pairs.push((key.clone(), value.clone()));
                } else {
                    pairs.push((key.clone(), Bytes::new()));
                }
                if pairs.len() >= limit {
                    break;
                }
            }
        }
        KvResponse::Range { pairs }
    }

    /// Explicit stale local read. Callers must never feed this to the
    /// linearizability checker.
    #[must_use]
    pub fn read_stale(&self, key: &[u8]) -> KvResponse {
        self.read_register(key)
    }

    /// Returns an `O(1)` snapshot handle. The persistent map clone shares
    /// structure, so this never blocks the consensus path. Serialization
    /// walks live entries once, off the path, through `encode_snapshot`.
    #[must_use]
    pub fn snapshot_handle(&self) -> Self {
        self.clone()
    }

    /// Returns the index this snapshot restores, always the applied index.
    /// Snapshots never describe state beyond what has applied.
    #[must_use]
    pub const fn snapshot_index(&self) -> LogIndex {
        self.applied_index
    }

    /// Returns the number of live client sessions in the snapshot.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Encodes a full v04 snapshot: map, tombstones, session table, config,
    /// membership section, and state hash, at the applied index.
    #[must_use]
    pub fn encode_snapshot(&self, membership: &MembershipSnapshot) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(crate::snapshot::SNAPSHOT_MAGIC_V04);
        buf.extend_from_slice(&self.virtual_tick.to_le_bytes());
        buf.extend_from_slice(&self.applied_index.as_u64().to_le_bytes());
        buf.extend_from_slice(&self.state_hash);
        encode_config(&self.config, &mut buf);
        let count = self.map.len() as u64;
        buf.extend_from_slice(&count.to_le_bytes());
        for (key, record) in &self.map {
            buf.extend_from_slice(&(key.len() as u64).to_le_bytes());
            buf.extend_from_slice(key);
            buf.push(u8::from(record.is_list));
            buf.extend_from_slice(&record.version.as_u64().to_le_bytes());
            buf.extend_from_slice(&(record.elements.len() as u64).to_le_bytes());
            for element in &record.elements {
                buf.extend_from_slice(&(element.len() as u64).to_le_bytes());
                buf.extend_from_slice(element);
            }
        }
        buf.extend_from_slice(&(self.tombstones.len() as u64).to_le_bytes());
        for (key, version) in &self.tombstones {
            buf.extend_from_slice(&(key.len() as u64).to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&version.as_u64().to_le_bytes());
        }
        encode_sessions(
            &self.sessions,
            &self.session_order,
            &mut buf,
            self.config.max_key_bytes,
            self.config.max_value_bytes,
        );
        membership.encode(&mut buf);
        Bytes::from(buf)
    }

    /// Encodes the snapshot into chunks with per-chunk CRC32C.
    ///
    /// Call on a handle from `snapshot_handle`, off the consensus path.
    /// Returns the chunks and the BLAKE3 digest of the full byte stream.
    #[must_use]
    pub fn encode_snapshot_chunked(
        &self,
        chunk_bytes: usize,
        membership: &MembershipSnapshot,
    ) -> (Vec<SnapshotChunk>, [u8; 32]) {
        let full = self.encode_snapshot(membership);
        let digest = snapshot_digest(&full);
        let chunks = split_into_chunks(&full, chunk_bytes);
        (chunks, digest)
    }

    /// Reassembles verified chunks and decodes the snapshot.
    ///
    /// Verifies per-chunk CRC, concatenation order, and the final digest
    /// before decoding. Rejects snapshots whose embedded config differs from
    /// `config`.
    pub fn decode_snapshot_chunked(
        config: KvConfig,
        chunks: &[SnapshotChunk],
        expected_digest: &[u8; 32],
    ) -> Result<Self, KvError> {
        let full = reassemble_chunks(chunks).map_err(|err| match err {
            ChunkError::Empty | ChunkError::Gap | ChunkError::TooMany => KvError::Decode,
            ChunkError::Corrupt => KvError::CorruptChunk,
        })?;
        if snapshot_digest(&full) != *expected_digest {
            return Err(KvError::CorruptChunk);
        }
        Self::decode_snapshot(config, &full)
    }

    /// Decodes a snapshot produced by `encode_snapshot`. Accepts v04 (with
    /// the membership section) and the legacy v03 and v02 formats. Bounds
    /// are checked before any reservation.
    pub fn decode_snapshot(config: KvConfig, input: &[u8]) -> Result<Self, KvError> {
        if let Some(rest) = input.strip_prefix(crate::snapshot::SNAPSHOT_MAGIC_V04) {
            return Self::decode_v04(config, rest);
        }
        if let Some(rest) = input.strip_prefix(crate::snapshot::SNAPSHOT_MAGIC_V03) {
            return Self::decode_v03(config, rest);
        }
        if let Some(rest) = input.strip_prefix(crate::snapshot::SNAPSHOT_MAGIC_V02) {
            return Self::decode_v02(config, rest);
        }
        Err(KvError::Decode)
    }

    /// Extracts the membership section of a snapshot. v04 carries the live
    /// config; older formats predate cluster membership and decode empty.
    ///
    /// # Errors
    /// Returns [`KvError::Decode`] on unknown magics or malformed sections.
    pub fn decode_membership(
        config: KvConfig,
        input: &[u8],
    ) -> Result<MembershipSnapshot, KvError> {
        if let Some(rest) = input.strip_prefix(crate::snapshot::SNAPSHOT_MAGIC_V04) {
            let (_, mut cursor) = Self::parse_v03_body(config, rest)?;
            return MembershipSnapshot::decode(&mut cursor).map_err(|_| KvError::Decode);
        }
        if input
            .strip_prefix(crate::snapshot::SNAPSHOT_MAGIC_V03)
            .is_some()
            || input
                .strip_prefix(crate::snapshot::SNAPSHOT_MAGIC_V02)
                .is_some()
        {
            return Ok(MembershipSnapshot::empty());
        }
        Err(KvError::Decode)
    }

    fn decode_v04(config: KvConfig, rest: &[u8]) -> Result<Self, KvError> {
        let (store, mut cursor) = Self::parse_v03_body(config, rest)?;
        MembershipSnapshot::decode(&mut cursor).map_err(|_| KvError::Decode)?;
        Ok(store)
    }

    fn decode_v03(config: KvConfig, rest: &[u8]) -> Result<Self, KvError> {
        let (store, cursor) = Self::parse_v03_body(config, rest)?;
        if !cursor.is_empty() {
            return Err(KvError::Decode);
        }
        Ok(store)
    }

    /// Parses the v03 body shared by v03 and v04, returning the store and
    /// the unconsumed tail (empty for v03, the membership section for v04).
    fn parse_v03_body(config: KvConfig, rest: &[u8]) -> Result<(Self, &[u8]), KvError> {
        if rest.len() < 56 {
            return Err(KvError::Decode);
        }
        let virtual_tick = u64::from_le_bytes(rest[0..8].try_into().map_err(|_| KvError::Decode)?);
        let applied = u64::from_le_bytes(rest[8..16].try_into().map_err(|_| KvError::Decode)?);
        let mut state_hash = [0u8; 32];
        state_hash.copy_from_slice(&rest[16..48]);
        let mut cursor = &rest[48..];
        let snapshot_config = decode_config(&mut cursor)?;
        if snapshot_config != config {
            return Err(KvError::Decode);
        }
        let count = read_u64(&mut cursor)? as usize;
        if count > 1_000_000 {
            return Err(KvError::Decode);
        }
        let mut map = imbl::OrdMap::new();
        for _ in 0..count {
            let key_len = read_u64(&mut cursor)? as usize;
            if key_len > config.max_key_bytes || cursor.len() < key_len {
                return Err(KvError::Decode);
            }
            let key = Bytes::copy_from_slice(&cursor[..key_len]);
            cursor = &cursor[key_len..];
            if cursor.is_empty() {
                return Err(KvError::Decode);
            }
            let is_list = cursor[0] != 0;
            cursor = &cursor[1..];
            let version = LogIndex::from_raw(read_u64(&mut cursor)?);
            let elements_len = read_u64(&mut cursor)? as usize;
            if elements_len > 1_000_000 {
                return Err(KvError::Decode);
            }
            let mut elements = Vec::with_capacity(elements_len.min(16));
            for _ in 0..elements_len {
                let value_len = read_u64(&mut cursor)? as usize;
                if value_len > config.max_value_bytes || cursor.len() < value_len {
                    return Err(KvError::Decode);
                }
                elements.push(Bytes::copy_from_slice(&cursor[..value_len]));
                cursor = &cursor[value_len..];
            }
            map.insert(
                key,
                KvRecord {
                    is_list,
                    elements,
                    version,
                },
            );
        }
        if cursor.is_empty() {
            return Err(KvError::Decode);
        }
        let tomb_count = read_u64(&mut cursor)? as usize;
        if tomb_count > 1_000_000 {
            return Err(KvError::Decode);
        }
        let mut tombstones = BTreeMap::new();
        for _ in 0..tomb_count {
            let key_len = read_u64(&mut cursor)? as usize;
            if key_len > config.max_key_bytes || cursor.len() < key_len {
                return Err(KvError::Decode);
            }
            let key = Bytes::copy_from_slice(&cursor[..key_len]);
            cursor = &cursor[key_len..];
            let version = LogIndex::from_raw(read_u64(&mut cursor)?);
            tombstones.insert(key, version);
        }
        let (sessions, session_order) = decode_sessions(
            &mut cursor,
            config.max_sessions,
            config.max_responses_per_session,
            config.max_key_bytes,
            config.max_value_bytes,
        )?;
        Ok((
            Self {
                config,
                map,
                sessions,
                session_order,
                virtual_tick,
                state_hash,
                applied_index: LogIndex::from_raw(applied),
                tombstones,
            },
            cursor,
        ))
    }

    fn decode_v02(config: KvConfig, rest: &[u8]) -> Result<Self, KvError> {
        if rest.len() < 56 {
            return Err(KvError::Decode);
        }
        let virtual_tick = u64::from_le_bytes(rest[0..8].try_into().map_err(|_| KvError::Decode)?);
        let applied = u64::from_le_bytes(rest[8..16].try_into().map_err(|_| KvError::Decode)?);
        let mut state_hash = [0u8; 32];
        state_hash.copy_from_slice(&rest[16..48]);
        let count = u64::from_le_bytes(rest[48..56].try_into().map_err(|_| KvError::Decode)?);
        let mut cursor = &rest[56..];
        let mut map = imbl::OrdMap::new();
        for _ in 0..count {
            let key_len = read_u64(&mut cursor)? as usize;
            if key_len > config.max_key_bytes || cursor.len() < key_len {
                return Err(KvError::Decode);
            }
            let key = Bytes::copy_from_slice(&cursor[..key_len]);
            cursor = &cursor[key_len..];
            if cursor.is_empty() {
                return Err(KvError::Decode);
            }
            let is_list = cursor[0] != 0;
            cursor = &cursor[1..];
            let version = LogIndex::from_raw(read_u64(&mut cursor)?);
            let elements_len = read_u64(&mut cursor)? as usize;
            if elements_len > 1_000_000 {
                return Err(KvError::Decode);
            }
            let mut elements = Vec::with_capacity(elements_len.min(16));
            for _ in 0..elements_len {
                let value_len = read_u64(&mut cursor)? as usize;
                if value_len > config.max_value_bytes || cursor.len() < value_len {
                    return Err(KvError::Decode);
                }
                elements.push(Bytes::copy_from_slice(&cursor[..value_len]));
                cursor = &cursor[value_len..];
            }
            map.insert(
                key,
                KvRecord {
                    is_list,
                    elements,
                    version,
                },
            );
        }
        let mut tombstones = BTreeMap::new();
        if !cursor.is_empty() {
            let tomb_count = read_u64(&mut cursor)? as usize;
            if tomb_count > 1_000_000 {
                return Err(KvError::Decode);
            }
            for _ in 0..tomb_count {
                let key_len = read_u64(&mut cursor)? as usize;
                if key_len > config.max_key_bytes || cursor.len() < key_len {
                    return Err(KvError::Decode);
                }
                let key = Bytes::copy_from_slice(&cursor[..key_len]);
                cursor = &cursor[key_len..];
                let version = LogIndex::from_raw(read_u64(&mut cursor)?);
                tombstones.insert(key, version);
            }
        }
        if !cursor.is_empty() {
            return Err(KvError::Decode);
        }
        Ok(Self {
            config,
            map,
            sessions: BTreeMap::new(),
            session_order: BTreeMap::new(),
            virtual_tick,
            state_hash,
            applied_index: LogIndex::from_raw(applied),
            tombstones,
        })
    }

    fn check_bounds(&self, key: &[u8], value: &[u8]) -> Result<(), KvError> {
        if key.len() > self.config.max_key_bytes {
            return Err(KvError::KeyTooLarge(key.len()));
        }
        if value.len() > self.config.max_value_bytes {
            return Err(KvError::ValueTooLarge(value.len()));
        }
        Ok(())
    }

    fn register_session(&mut self, session: SessionId) -> Result<(), KvError> {
        if self.sessions.contains_key(&session) {
            return Ok(());
        }
        if self.sessions.len() >= self.config.max_sessions {
            self.evict_oldest_session();
        }
        if self.sessions.len() >= self.config.max_sessions {
            return Err(KvError::TooManySessions);
        }
        self.sessions.insert(
            session,
            SessionState {
                last_activity: self.virtual_tick,
                responses: BTreeMap::new(),
                highest_seq: 0,
            },
        );
        self.session_order.insert(session, self.virtual_tick);
        Ok(())
    }

    fn touch_session(&mut self, session: SessionId, seq: SeqNo) -> Result<(), KvError> {
        let Some(state) = self.sessions.get_mut(&session) else {
            return Err(KvError::TooManySessions);
        };
        state.last_activity = self.virtual_tick;
        if seq.as_u64() > state.highest_seq {
            state.highest_seq = seq.as_u64();
        }
        self.session_order.insert(session, self.virtual_tick);
        Ok(())
    }

    fn dedup_lookup(&mut self, session: SessionId, seq: SeqNo, acked: SeqNo) -> Option<KvResponse> {
        let state = self.sessions.get_mut(&session)?;
        state.last_activity = self.virtual_tick;
        retain_acked(&mut state.responses, acked);
        state.responses.get(&seq).cloned()
    }

    fn memoize(&mut self, session: SessionId, seq: SeqNo, response: &KvResponse) {
        if let Some(state) = self.sessions.get_mut(&session) {
            state.responses.insert(seq, response.clone());
            while state.responses.len() > self.config.max_responses_per_session {
                let Some(first) = state.responses.keys().next().copied() else {
                    break;
                };
                state.responses.remove(&first);
            }
        }
    }

    fn expire_sessions(&mut self, up_to_tick: u64) -> u64 {
        let idle = self.config.session_idle_ticks;
        let expired: Vec<SessionId> = self
            .sessions
            .iter()
            .filter_map(|(id, state)| {
                if up_to_tick.saturating_sub(state.last_activity) >= idle {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        let removed = expired.len() as u64;
        for id in expired {
            self.sessions.remove(&id);
            self.session_order.remove(&id);
        }
        removed
    }

    fn evict_oldest_session(&mut self) {
        let oldest = self
            .session_order
            .iter()
            .min_by_key(|(_, tick)| **tick)
            .map(|(id, _)| *id);
        if let Some(id) = oldest {
            self.sessions.remove(&id);
            self.session_order.remove(&id);
        }
    }

    fn mix_hash(&mut self, index: LogIndex, tag: u8, key: &[u8], value: &[u8]) {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.state_hash);
        hasher.update(&index.as_u64().to_le_bytes());
        hasher.update(&[tag]);
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key);
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value);
        let digest = hasher.finalize();
        self.state_hash.copy_from_slice(digest.as_bytes());
    }
}

fn retain_acked(responses: &mut BTreeMap<SeqNo, KvResponse>, acked: SeqNo) {
    let stale: Vec<SeqNo> = responses
        .keys()
        .copied()
        .filter(|seq| seq.as_u64() <= acked.as_u64())
        .collect();
    for seq in stale {
        responses.remove(&seq);
    }
}

fn read_u64(cursor: &mut &[u8]) -> Result<u64, KvError> {
    if cursor.len() < 8 {
        return Err(KvError::Decode);
    }
    let value = u64::from_le_bytes(cursor[..8].try_into().map_err(|_| KvError::Decode)?);
    *cursor = &cursor[8..];
    Ok(value)
}

fn encode_config(config: &KvConfig, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(config.max_key_bytes as u64).to_le_bytes());
    buf.extend_from_slice(&(config.max_value_bytes as u64).to_le_bytes());
    buf.extend_from_slice(&config.session_idle_ticks.to_le_bytes());
    buf.extend_from_slice(&(config.max_sessions as u64).to_le_bytes());
    buf.extend_from_slice(&(config.max_responses_per_session as u64).to_le_bytes());
}

fn decode_config(cursor: &mut &[u8]) -> Result<KvConfig, KvError> {
    let max_key = read_u64(cursor)?;
    let max_value = read_u64(cursor)?;
    let idle = read_u64(cursor)?;
    let max_sessions = read_u64(cursor)?;
    let max_responses = read_u64(cursor)?;
    let max_key = usize::try_from(max_key).map_err(|_| KvError::Decode)?;
    let max_value = usize::try_from(max_value).map_err(|_| KvError::Decode)?;
    let max_sessions = usize::try_from(max_sessions).map_err(|_| KvError::Decode)?;
    let max_responses = usize::try_from(max_responses).map_err(|_| KvError::Decode)?;
    if max_key == 0 || max_key > 16 * 1024 * 1024 {
        return Err(KvError::Decode);
    }
    if max_value == 0 || max_value > 64 * 1024 * 1024 {
        return Err(KvError::Decode);
    }
    if max_sessions == 0 || max_sessions > 1_000_000 {
        return Err(KvError::Decode);
    }
    if max_responses == 0 || max_responses > 100_000 {
        return Err(KvError::Decode);
    }
    Ok(KvConfig {
        max_key_bytes: max_key,
        max_value_bytes: max_value,
        session_idle_ticks: idle,
        max_sessions,
        max_responses_per_session: max_responses,
    })
}

fn encode_sessions(
    sessions: &BTreeMap<SessionId, SessionState>,
    order: &BTreeMap<SessionId, u64>,
    buf: &mut Vec<u8>,
    max_key: usize,
    max_value: usize,
) {
    buf.extend_from_slice(&(sessions.len() as u64).to_le_bytes());
    for (id, state) in sessions {
        buf.extend_from_slice(&id.as_u64().to_le_bytes());
        buf.extend_from_slice(&state.last_activity.to_le_bytes());
        buf.extend_from_slice(&state.highest_seq.to_le_bytes());
        buf.extend_from_slice(&(state.responses.len() as u64).to_le_bytes());
        for (seq, response) in &state.responses {
            buf.extend_from_slice(&seq.as_u64().to_le_bytes());
            let mut tmp = Vec::new();
            encode_response(response, &mut tmp, max_key, max_value);
            buf.extend_from_slice(&(tmp.len() as u64).to_le_bytes());
            buf.extend_from_slice(&tmp);
        }
        let tick = order.get(id).copied().unwrap_or(state.last_activity);
        buf.extend_from_slice(&tick.to_le_bytes());
    }
}

/// Live sessions plus their ordering ticks restored from a snapshot.
type SessionTables = (BTreeMap<SessionId, SessionState>, BTreeMap<SessionId, u64>);

fn decode_sessions(
    cursor: &mut &[u8],
    max_sessions: usize,
    max_responses: usize,
    max_key: usize,
    max_value: usize,
) -> Result<SessionTables, KvError> {
    let count = read_u64(cursor)? as usize;
    if count > max_sessions {
        return Err(KvError::Decode);
    }
    let mut sessions = BTreeMap::new();
    let mut order = BTreeMap::new();
    for _ in 0..count {
        let id = SessionId::from_raw(read_u64(cursor)?);
        let last_activity = read_u64(cursor)?;
        let highest_seq = read_u64(cursor)?;
        let responses_len = read_u64(cursor)? as usize;
        if responses_len > max_responses {
            return Err(KvError::Decode);
        }
        let mut responses = BTreeMap::new();
        for _ in 0..responses_len {
            let seq = SeqNo::from_raw(read_u64(cursor)?);
            let len = read_u64(cursor)? as usize;
            if len > 8 * 1024 * 1024 || cursor.len() < len {
                return Err(KvError::Decode);
            }
            let response = decode_response(&cursor[..len], max_key, max_value)?;
            *cursor = &cursor[len..];
            responses.insert(seq, response);
        }
        let tick = read_u64(cursor)?;
        if sessions
            .insert(
                id,
                SessionState {
                    last_activity,
                    responses,
                    highest_seq,
                },
            )
            .is_some()
        {
            return Err(KvError::Decode);
        }
        order.insert(id, tick);
    }
    Ok((sessions, order))
}

fn encode_response(response: &KvResponse, buf: &mut Vec<u8>, _max_key: usize, _max_value: usize) {
    match response {
        KvResponse::Ok => buf.push(0),
        KvResponse::SessionRegistered { session } => {
            buf.push(1);
            buf.extend_from_slice(&session.as_u64().to_le_bytes());
        }
        KvResponse::Value { value, version } => {
            buf.push(2);
            match value {
                Some(v) => {
                    buf.push(1);
                    buf.extend_from_slice(&(v.len() as u64).to_le_bytes());
                    buf.extend_from_slice(v);
                }
                None => buf.push(0),
            }
            buf.extend_from_slice(&version.as_u64().to_le_bytes());
        }
        KvResponse::List { values, version } => {
            buf.push(3);
            buf.extend_from_slice(&(values.len() as u64).to_le_bytes());
            for v in values {
                buf.extend_from_slice(&(v.len() as u64).to_le_bytes());
                buf.extend_from_slice(v);
            }
            buf.extend_from_slice(&version.as_u64().to_le_bytes());
        }
        KvResponse::Range { pairs } => {
            buf.push(4);
            buf.extend_from_slice(&(pairs.len() as u64).to_le_bytes());
            for (k, v) in pairs {
                buf.extend_from_slice(&(k.len() as u64).to_le_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(&(v.len() as u64).to_le_bytes());
                buf.extend_from_slice(v);
            }
        }
        KvResponse::CasConflict { version } => {
            buf.push(5);
            buf.extend_from_slice(&version.as_u64().to_le_bytes());
        }
        KvResponse::SessionExpired => buf.push(6),
        KvResponse::UnknownSession => buf.push(7),
        KvResponse::Duplicate => buf.push(8),
        KvResponse::SessionsExpired { removed } => {
            buf.push(9);
            buf.extend_from_slice(&removed.to_le_bytes());
        }
    }
}

fn decode_response(input: &[u8], max_key: usize, max_value: usize) -> Result<KvResponse, KvError> {
    let (tag, rest) = input.split_first().ok_or(KvError::Decode)?;
    match tag {
        0 => {
            if !rest.is_empty() {
                return Err(KvError::Decode);
            }
            Ok(KvResponse::Ok)
        }
        1 => {
            if rest.len() != 8 {
                return Err(KvError::Decode);
            }
            let raw = u64::from_le_bytes(rest.try_into().map_err(|_| KvError::Decode)?);
            Ok(KvResponse::SessionRegistered {
                session: SessionId::from_raw(raw),
            })
        }
        2 => decode_value_response(rest, max_value),
        3 => decode_list_response(rest, max_value),
        4 => decode_range_response(rest, max_key, max_value),
        5 => {
            if rest.len() != 8 {
                return Err(KvError::Decode);
            }
            let version = LogIndex::from_raw(u64::from_le_bytes(
                rest.try_into().map_err(|_| KvError::Decode)?,
            ));
            Ok(KvResponse::CasConflict { version })
        }
        6 => {
            if !rest.is_empty() {
                return Err(KvError::Decode);
            }
            Ok(KvResponse::SessionExpired)
        }
        7 => {
            if !rest.is_empty() {
                return Err(KvError::Decode);
            }
            Ok(KvResponse::UnknownSession)
        }
        8 => {
            if !rest.is_empty() {
                return Err(KvError::Decode);
            }
            Ok(KvResponse::Duplicate)
        }
        9 => {
            if rest.len() != 8 {
                return Err(KvError::Decode);
            }
            let removed = u64::from_le_bytes(rest.try_into().map_err(|_| KvError::Decode)?);
            Ok(KvResponse::SessionsExpired { removed })
        }
        _ => Err(KvError::Decode),
    }
}

fn decode_value_response(mut rest: &[u8], max_value: usize) -> Result<KvResponse, KvError> {
    if rest.is_empty() {
        return Err(KvError::Decode);
    }
    let has_value = rest[0] != 0;
    rest = &rest[1..];
    let value = if has_value {
        let len = read_u64(&mut rest)? as usize;
        if len > max_value || rest.len() < len {
            return Err(KvError::Decode);
        }
        let v = Bytes::copy_from_slice(&rest[..len]);
        rest = &rest[len..];
        Some(v)
    } else {
        None
    };
    if rest.len() != 8 {
        return Err(KvError::Decode);
    }
    let version = LogIndex::from_raw(u64::from_le_bytes(
        rest.try_into().map_err(|_| KvError::Decode)?,
    ));
    Ok(KvResponse::Value { value, version })
}

fn decode_list_response(mut rest: &[u8], max_value: usize) -> Result<KvResponse, KvError> {
    let count = read_u64(&mut rest)? as usize;
    if count > 1_000_000 {
        return Err(KvError::Decode);
    }
    let mut values = Vec::with_capacity(count.min(16));
    for _ in 0..count {
        let len = read_u64(&mut rest)? as usize;
        if len > max_value || rest.len() < len {
            return Err(KvError::Decode);
        }
        values.push(Bytes::copy_from_slice(&rest[..len]));
        rest = &rest[len..];
    }
    if rest.len() != 8 {
        return Err(KvError::Decode);
    }
    let version = LogIndex::from_raw(u64::from_le_bytes(
        rest.try_into().map_err(|_| KvError::Decode)?,
    ));
    Ok(KvResponse::List { values, version })
}

fn decode_range_response(
    mut rest: &[u8],
    max_key: usize,
    max_value: usize,
) -> Result<KvResponse, KvError> {
    let count = read_u64(&mut rest)? as usize;
    if count > 100_000 {
        return Err(KvError::Decode);
    }
    let mut pairs = Vec::with_capacity(count.min(16));
    for _ in 0..count {
        let key_len = read_u64(&mut rest)? as usize;
        if key_len > max_key || rest.len() < key_len {
            return Err(KvError::Decode);
        }
        let key = Bytes::copy_from_slice(&rest[..key_len]);
        rest = &rest[key_len..];
        let value_len = read_u64(&mut rest)? as usize;
        if value_len > max_value || rest.len() < value_len {
            return Err(KvError::Decode);
        }
        let value = Bytes::copy_from_slice(&rest[..value_len]);
        rest = &rest[value_len..];
        pairs.push((key, value));
    }
    if !rest.is_empty() {
        return Err(KvError::Decode);
    }
    Ok(KvResponse::Range { pairs })
}
