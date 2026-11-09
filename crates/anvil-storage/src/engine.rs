#![forbid(unsafe_code)]

//! Sans-IO Storage Engine with natural batching group commit and crash recovery.

use crate::config::StorageConfig;
use crate::device::{CompletionPayload, DeviceCompletion, DeviceOp, FileId, SectorSize};
use crate::error::StorageError;
use crate::format::hard_state::{HardStateRecoveryOutcome, HardStateSlot, recover_hard_state};
use crate::format::wal::{
    WAL_RECORD_HEADER_FIXED_LEN, WalRecordFrame, WalScanResult, WalSegmentHeader, decode_wal_frame,
};
use crate::recovery::{
    FramePlacement, LogRecoveryClassification, RecoveredLog, RecoveredStorageState,
};
use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};

/// Input events fed into `StorageEngine::step`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineInput {
    /// Update persistent Raft hard state (term and vote).
    WriteHardState {
        /// Raft consensus term.
        term: Term,
        /// Node ID voted for, if any.
        vote: Option<NodeId>,
        /// Persistence token to acknowledge once synced.
        token: u64,
    },
    /// Append sequential log entries to the active WAL segment.
    AppendEntries {
        /// Expected index preceding the first appended entry.
        prev_index: LogIndex,
        /// New entries to persist.
        entries: Vec<Entry>,
        /// Persistence token to acknowledge once synced.
        token: u64,
    },
    /// Truncate conflicting suffix starting at `from_index` and append new entries.
    TruncateAndAppend {
        /// First log index to replace / overwrite.
        from_index: LogIndex,
        /// Replacement entries.
        entries: Vec<Entry>,
        /// Persistence token to acknowledge once synced.
        token: u64,
    },
    /// Completion event returned by the underlying abstract `Device`.
    Completion(DeviceCompletion),
}

/// Output actions produced by `StorageEngine::step`.
#[derive(Debug, Default)]
pub struct EngineOutput {
    /// Device I/O operations to submit.
    pub device_ops: Vec<DeviceOp>,
    /// Persistence tokens that have been guaranteed durable on disk (in submission order).
    pub persisted_tokens: Vec<u64>,
}

impl EngineOutput {
    /// Resets all output buffers for reuse without deallocating backing memory.
    pub fn clear(&mut self) {
        self.device_ops.clear();
        self.persisted_tokens.clear();
    }
}

/// A hard-state update whose sync has been submitted but not yet verified.
#[derive(Debug, Clone)]
struct HardStateSync {
    /// Device op id of the `Fdatasync` that persists this update.
    sync_op_id: u64,
    /// Token to acknowledge once the update is verified durable.
    token: u64,
    /// Slot index the update was written to.
    slot: u16,
    /// Sequence number written with the update.
    sequence_no: u64,
    /// Term written with the update.
    term: Term,
    /// Vote written with the update.
    vote: Option<NodeId>,
}

/// A hard-state verification read awaiting comparison against the written values.
#[derive(Debug, Clone)]
struct HardStateVerify {
    /// Device op id of the verification `Read`.
    read_op_id: u64,
    /// Token to acknowledge once verification succeeds.
    token: u64,
    /// Slot index the update was written to.
    slot: u16,
    /// Expected sequence number.
    sequence_no: u64,
    /// Expected term.
    term: Term,
    /// Expected vote.
    vote: Option<NodeId>,
}

/// Sans-IO consensus storage engine.
pub struct StorageEngine {
    config: StorageConfig,
    next_op_id: u64,

    // Active hard state
    active_hard_state_slot: usize,
    hard_state_seq: u64,
    current_term: Term,
    voted_for: Option<NodeId>,

    // Active WAL segment
    active_segment_id: u64,
    active_segment_file_id: FileId,
    segment_write_offset: u64,
    last_entry_index: LogIndex,
    last_entry_term: Term,
    last_record_crc: u32,

    // Natural batching group commit state (WAL)
    in_flight_wal_syncs: VecDeque<(u64, Vec<u64>)>,
    completed_wal_syncs: std::collections::BTreeSet<u64>,

    // In-flight hard state sync operations awaiting verification reads
    hard_state_syncs: Vec<HardStateSync>,
    // In-flight hard state verification reads awaiting comparison
    hard_state_verifies: Vec<HardStateVerify>,

    // Pending coalesced batch (while sync is in flight)
    pending_tokens: Vec<u64>,
    pending_write_buffer: Vec<u8>,
    pending_write_start_offset: u64,
    // Byte lengths of staged but unsynced frames, in index order. Dropping a
    // conflicting tail truncates the buffer by the matching suffix of lengths.
    pending_frame_lens: Vec<usize>,

    // Byte-exact journal of every live frame: segment, offset, chained CRC
    // seeds, and term per index, in index order starting at 1. A Vec keeps
    // steady-state appends allocation-free after capacity is reserved;
    // staging records new frames, recovery rebuilds from observed placements,
    // and truncation rewinds by truncating the tail.
    index_journal: Vec<FramePlacement>,
    // Sealed segments as (id, first index, last index), oldest first.
    sealed_segments: Vec<(u64, LogIndex, LogIndex)>,
    // Header CRC per live segment id, for truncating a segment to its head.
    seg_header_crcs: BTreeMap<u64, u32>,
    // First index and header CRC of the active segment.
    active_first_index: LogIndex,
    active_header_crc: u32,

    // Unrecoverable fatal error (e.g. fsync failure under ADR 0013)
    fatal_error: Option<StorageError>,
}

impl StorageEngine {
    /// Creates a new `StorageEngine` initialized from recovered storage state.
    #[must_use]
    pub fn new(
        config: StorageConfig,
        recovered: &RecoveredStorageState,
        active_segment_id: u64,
        segment_write_offset: u64,
        last_record_crc: u32,
    ) -> Self {
        let (active_slot, seq) = match recovered.hard_state_outcome {
            HardStateRecoveryOutcome::BothValid {
                active_slot,
                sequence_no,
                ..
            }
            | HardStateRecoveryOutcome::OneSlotCorrupted {
                valid_slot: active_slot,
                sequence_no,
                ..
            } => (active_slot, sequence_no),
        };

        let mut engine = Self {
            config,
            next_op_id: 1,
            active_hard_state_slot: active_slot,
            hard_state_seq: seq,
            current_term: recovered.current_term,
            voted_for: recovered.voted_for,
            active_segment_id,
            active_segment_file_id: FileId::wal_segment(active_segment_id),
            segment_write_offset,
            last_entry_index: recovered.log.last_index,
            last_entry_term: recovered.log.last_term,
            last_record_crc,
            in_flight_wal_syncs: VecDeque::new(),
            completed_wal_syncs: std::collections::BTreeSet::new(),
            hard_state_syncs: Vec::with_capacity(4),
            hard_state_verifies: Vec::with_capacity(4),
            pending_tokens: Vec::with_capacity(64),
            pending_write_buffer: Vec::with_capacity(64 * 1024),
            pending_write_start_offset: segment_write_offset,
            pending_frame_lens: Vec::with_capacity(256),
            index_journal: Self::journal_from_placements(&recovered.log.placements),
            sealed_segments: Self::sealed_from_placements(
                &recovered.log.placements,
                active_segment_id,
            ),
            seg_header_crcs: Self::header_crcs_from_placements(&recovered.log.placements),
            active_first_index: Self::active_first_from_placements(
                &recovered.log.placements,
                active_segment_id,
                recovered.log.last_index,
            ),
            active_header_crc: Self::active_header_from_placements(
                &recovered.log.placements,
                active_segment_id,
                last_record_crc,
            ),
            fatal_error: None,
        };
        // An active segment rewritten after a torn tail holds no frames yet,
        // so no placement seeds its header CRC. Record it explicitly: later
        // truncations address it by segment id.
        engine
            .seg_header_crcs
            .entry(active_segment_id)
            .or_insert_with(|| engine.active_header_crc);
        engine.assert_active_matches(segment_write_offset, last_record_crc);
        engine
    }

    /// Checks the journal-derived active end against the offsets the caller
    /// replayed. Both describe the same durable prefix and must agree.
    fn assert_active_matches(&self, segment_write_offset: u64, last_record_crc: u32) {
        let mut end_offset = self.config.sector_size.as_u64();
        let mut end_crc = self.active_header_crc;
        for placement in &self.index_journal {
            if placement.seg_id == self.active_segment_id {
                end_offset = placement.offset + placement.frame_len as u64;
                end_crc = placement.crc_after;
            }
        }
        assert_eq!(
            end_offset, segment_write_offset,
            "journal active end must match replayed write offset"
        );
        assert_eq!(
            end_crc, last_record_crc,
            "journal active CRC must match replayed CRC"
        );
    }

    /// Rebuilds the byte-exact frame journal from the recovery scan's placements.
    fn journal_from_placements(placements: &[FramePlacement]) -> Vec<FramePlacement> {
        for (pos, placement) in placements.iter().enumerate() {
            assert!(
                u64::try_from(pos).is_ok_and(|n| n + 1 == placement.index.as_u64()),
                "recovery placements must be dense from index 1"
            );
        }
        placements.to_vec()
    }

    /// Derives sealed segment ranges from placements, oldest first.
    fn sealed_from_placements(
        placements: &[FramePlacement],
        active_segment_id: u64,
    ) -> Vec<(u64, LogIndex, LogIndex)> {
        let mut ranges: Vec<(u64, LogIndex, LogIndex)> = Vec::new();
        for placement in placements {
            if placement.seg_id >= active_segment_id {
                continue;
            }
            match ranges.last_mut() {
                Some(last) if last.0 == placement.seg_id => {
                    last.2 = placement.index;
                }
                _ => ranges.push((placement.seg_id, placement.index, placement.index)),
            }
        }
        ranges
    }

    /// Derives per-segment header CRCs from the first placement of each segment.
    fn header_crcs_from_placements(placements: &[FramePlacement]) -> BTreeMap<u64, u32> {
        let mut crcs = BTreeMap::new();
        for placement in placements {
            crcs.entry(placement.seg_id).or_insert(placement.crc_before);
        }
        crcs
    }

    /// Derives the active segment's first index from placements.
    fn active_first_from_placements(
        placements: &[FramePlacement],
        active_segment_id: u64,
        last_index: LogIndex,
    ) -> LogIndex {
        placements
            .iter()
            .find(|p| p.seg_id == active_segment_id)
            .map_or(last_index.next().unwrap_or(last_index), |placement| {
                placement.index
            })
    }

    /// Derives the active segment's header CRC from placements.
    fn active_header_from_placements(
        placements: &[FramePlacement],
        active_segment_id: u64,
        resume_crc: u32,
    ) -> u32 {
        placements
            .iter()
            .find(|p| p.seg_id == active_segment_id)
            .map_or(resume_crc, |placement| placement.crc_before)
    }

    /// Initializes a fresh storage engine with empty state.
    #[must_use]
    pub fn new_empty(config: StorageConfig) -> Self {
        let sector_size = config.sector_size;
        let segment_id = 1;
        let dummy_header = WalSegmentHeader::new(
            sector_size,
            segment_id,
            LogIndex::from_raw(1),
            Term::from_raw(0),
        );

        Self {
            config,
            next_op_id: 1,
            active_hard_state_slot: 0,
            hard_state_seq: 0,
            current_term: Term::from_raw(0),
            voted_for: None,
            active_segment_id: segment_id,
            active_segment_file_id: FileId::wal_segment(segment_id),
            segment_write_offset: sector_size.as_u64(),
            last_entry_index: LogIndex::from_raw(0),
            last_entry_term: Term::from_raw(0),
            last_record_crc: dummy_header.header_crc,
            in_flight_wal_syncs: VecDeque::new(),
            completed_wal_syncs: std::collections::BTreeSet::new(),
            hard_state_syncs: Vec::with_capacity(4),
            hard_state_verifies: Vec::with_capacity(4),
            pending_tokens: Vec::with_capacity(64),
            pending_write_buffer: Vec::with_capacity(64 * 1024),
            pending_write_start_offset: sector_size.as_u64(),
            pending_frame_lens: Vec::with_capacity(256),
            index_journal: Vec::new(),
            sealed_segments: Vec::new(),
            seg_header_crcs: BTreeMap::from([(segment_id, dummy_header.header_crc)]),
            active_first_index: LogIndex::from_raw(1),
            active_header_crc: dummy_header.header_crc,
            fatal_error: None,
        }
    }

    /// Returns the active segment's first index.
    #[must_use]
    pub const fn active_first_index(&self) -> LogIndex {
        self.active_first_index
    }

    /// Returns the active segment ID.
    #[must_use]
    pub const fn active_segment_id(&self) -> u64 {
        self.active_segment_id
    }

    /// Returns the current write byte offset in the active segment.
    #[must_use]
    pub const fn segment_write_offset(&self) -> u64 {
        self.segment_write_offset
    }

    /// Returns the latest chained CRC32C.
    #[must_use]
    pub const fn last_record_crc(&self) -> u32 {
        self.last_record_crc
    }

    /// Returns the highest persisted log index.
    #[must_use]
    pub const fn last_entry_index(&self) -> LogIndex {
        self.last_entry_index
    }

    /// Pre-reserves capacity for pending tokens and write buffers to guarantee steady-state zero allocation.
    pub fn reserve_pending_capacity(&mut self, additional_tokens: usize, additional_bytes: usize) {
        self.pending_tokens.reserve(additional_tokens);
        self.pending_write_buffer.reserve(additional_bytes);
        self.pending_frame_lens.reserve(additional_tokens);
        self.index_journal.reserve(additional_tokens);
    }

    /// Returns the fatal storage error if one occurred.
    #[must_use]
    pub fn fatal_error(&self) -> Option<&StorageError> {
        self.fatal_error.as_ref()
    }

    /// Advances the internal operation sequence counter.
    fn next_op_id(&mut self) -> u64 {
        let op_id = self.next_op_id;
        self.next_op_id = self.next_op_id.wrapping_add(1);
        op_id
    }

    /// Main sans-IO step driver.
    pub fn step(&mut self, input: EngineInput, output: &mut EngineOutput) {
        if self.fatal_error.is_some() {
            return;
        }

        match input {
            EngineInput::WriteHardState { term, vote, token } => {
                self.handle_write_hard_state(term, vote, token, output);
            }
            EngineInput::AppendEntries {
                prev_index,
                entries,
                token,
            } => {
                self.handle_append_entries(prev_index, &entries, token, output);
            }
            EngineInput::TruncateAndAppend {
                from_index,
                entries,
                token,
            } => {
                self.handle_truncate_and_append(from_index, &entries, token, output);
            }
            EngineInput::Completion(completion) => {
                self.handle_completion(&completion, output);
            }
        }
    }

    fn handle_write_hard_state(
        &mut self,
        term: Term,
        vote: Option<NodeId>,
        token: u64,
        output: &mut EngineOutput,
    ) {
        // Alternate slot: 0 -> 1, 1 -> 0
        let target_slot = 1 - self.active_hard_state_slot;
        self.hard_state_seq += 1;
        self.current_term = term;
        self.voted_for = vote;

        let target_slot_u16 = u16::from(target_slot != 0);
        let slot = HardStateSlot::new(target_slot_u16, self.hard_state_seq, term, vote);
        let sector_size = self.config.sector_size;
        let mut slot_buf = vec![0u8; sector_size.as_usize()];
        assert!(
            slot.encode(sector_size, &mut slot_buf).is_ok(),
            "hard-state slot buffer is sector sized"
        );

        let target_offset = (target_slot as u64) * sector_size.as_u64();
        let write_op_id = self.next_op_id();
        output.device_ops.push(DeviceOp::Write {
            op_id: write_op_id,
            file_id: FileId::HARD_STATE,
            offset: target_offset,
            payload: Bytes::from(slot_buf),
        });

        self.active_hard_state_slot = target_slot;

        let sync_op_id = self.next_op_id();
        output.device_ops.push(DeviceOp::Fdatasync {
            op_id: sync_op_id,
            file_id: FileId::HARD_STATE,
        });
        self.hard_state_syncs.push(HardStateSync {
            sync_op_id,
            token,
            slot: target_slot_u16,
            sequence_no: self.hard_state_seq,
            term,
            vote,
        });
    }

    fn handle_append_entries(
        &mut self,
        prev_index: LogIndex,
        entries: &[Entry],
        token: u64,
        output: &mut EngineOutput,
    ) {
        assert_eq!(
            prev_index, self.last_entry_index,
            "storage engine append must be strictly sequential"
        );

        if entries.is_empty() {
            if self.in_flight_wal_syncs.is_empty() {
                output.persisted_tokens.push(token);
            } else {
                self.pending_tokens.push(token);
            }
            return;
        }

        self.stage_entries(prev_index, entries, token, output);
    }

    /// Stages entries into the pending batch, rotating the segment when the
    /// batch no longer fits. Records every frame's journal placement and
    /// pending length so a later truncation rewinds byte-exact state.
    fn stage_entries(
        &mut self,
        prev_index: LogIndex,
        entries: &[Entry],
        token: u64,
        output: &mut EngineOutput,
    ) {
        assert_eq!(
            prev_index, self.last_entry_index,
            "storage engine append must be strictly sequential"
        );

        let sector_size = self.config.sector_size;

        // Check if entries fit in active segment; if not, rotate
        let mut needed_bytes = 0usize;
        for entry in entries {
            needed_bytes += WalRecordFrame::compute_frame_size(entry.payload.len(), sector_size);
        }

        if self.segment_write_offset + (needed_bytes as u64) > self.config.segment_capacity_bytes {
            self.rotate_segment(output);
        }

        // Stage all entries into pending_write_buffer
        if self.pending_write_buffer.is_empty() {
            self.pending_write_start_offset = self.segment_write_offset;
        }

        for entry in entries {
            let frame = WalRecordFrame::from_entry(
                entry,
                self.last_record_crc,
                self.active_segment_id,
                self.segment_write_offset,
            );
            let frame_len = frame.frame_size(sector_size);
            frame.encode_frame(sector_size, &mut self.pending_write_buffer);
            assert!(
                u64::try_from(self.index_journal.len())
                    .is_ok_and(|n| n + 1 == entry.index.as_u64()),
                "staged entries must extend the journal sequentially"
            );
            self.index_journal.push(FramePlacement {
                index: entry.index,
                seg_id: self.active_segment_id,
                offset: self.segment_write_offset,
                crc_before: self.last_record_crc,
                crc_after: frame.crc32c,
                term: entry.term,
                frame_len,
            });
            self.pending_frame_lens.push(frame_len);
            self.last_record_crc = frame.crc32c;
            self.segment_write_offset += frame_len as u64;
            self.last_entry_index = entry.index;
            self.last_entry_term = entry.term;
        }

        self.pending_tokens.push(token);

        // Natural batching: if no sync is in flight, flush immediately
        if self.in_flight_wal_syncs.is_empty() {
            self.flush_pending_batch(output);
        }
    }

    fn handle_truncate_and_append(
        &mut self,
        from_index: LogIndex,
        entries: &[Entry],
        token: u64,
        output: &mut EngineOutput,
    ) {
        assert!(
            from_index.as_u64() >= 1,
            "from_index must be >= 1 for truncation"
        );
        assert!(
            from_index.as_u64() <= self.last_entry_index.as_u64() + 1,
            "truncate point beyond log tip"
        );

        if from_index.as_u64() <= self.last_entry_index.as_u64() {
            self.rewind_to(
                from_index.checked_sub(1).unwrap_or(LogIndex::from_raw(0)),
                output,
            );
        }

        let target_prev_index = from_index.checked_sub(1).unwrap_or(LogIndex::from_raw(0));
        self.stage_entries(target_prev_index, entries, token, output);
    }

    /// Rewinds the log to `keep_last`, discarding every frame above it.
    ///
    /// Offsets, chained CRC seeds, terms, and the active segment come from the
    /// journal, so rewound state is byte-identical to staging only the kept
    /// prefix. Durable bytes beyond the rewind point are cut with a `Truncate`
    /// operation; sealed segments above the rewind point and the abandoned
    /// active file are unlinked. Pending
    /// tokens are retained and acknowledged with the next flush: responses for
    /// replaced entries are stale but idempotent, while dropping them would
    /// leak the Raft core's persist waits.
    fn rewind_to(&mut self, keep_last: LogIndex, output: &mut EngineOutput) {
        let first_dropped = keep_last.next().unwrap_or(keep_last);
        let dropped = self.journal_at(first_dropped).clone();
        let new_seg = dropped.seg_id;

        // Unlink every sealed segment above the rewind point, plus the
        // abandoned active file when the rewind leaves it.
        let mut unlinked: Vec<u64> = Vec::new();
        if self.active_segment_id != new_seg {
            unlinked.push(self.active_segment_id);
        }
        self.sealed_segments.retain(|(seg_id, _, _)| {
            if *seg_id > new_seg {
                unlinked.push(*seg_id);
                false
            } else {
                true
            }
        });
        for seg_id in unlinked {
            let unlink_op_id = self.next_op_id();
            output.device_ops.push(DeviceOp::Unlink {
                op_id: unlink_op_id,
                file_id: FileId::wal_segment(seg_id),
            });
            self.seg_header_crcs.remove(&seg_id);
        }
        if self.config.sync_directories && self.active_segment_id > new_seg {
            let sync_dir_op_id = self.next_op_id();
            output.device_ops.push(DeviceOp::SyncDir {
                op_id: sync_dir_op_id,
            });
        }

        // Truncate the surviving active file when rewound bytes were flushed.
        assert!(
            self.seg_header_crcs.contains_key(&new_seg),
            "rewind target segment must have a recorded header"
        );
        let (new_offset, new_crc) =
            if keep_last.as_u64() == 0 || keep_last < self.seg_first_index(new_seg) {
                (
                    self.config.sector_size.as_u64(),
                    self.seg_header_crcs[&new_seg],
                )
            } else {
                (dropped.offset, dropped.crc_before)
            };
        let flushed_end = if self.pending_write_buffer.is_empty() {
            self.segment_write_offset
        } else {
            self.pending_write_start_offset
        };
        if new_seg == self.active_segment_id && new_offset < flushed_end {
            let truncate_op_id = self.next_op_id();
            output.device_ops.push(DeviceOp::Truncate {
                op_id: truncate_op_id,
                file_id: self.active_segment_file_id,
                len: new_offset,
            });
        } else if new_seg != self.active_segment_id {
            let truncate_op_id = self.next_op_id();
            output.device_ops.push(DeviceOp::Truncate {
                op_id: truncate_op_id,
                file_id: FileId::wal_segment(new_seg),
                len: new_offset,
            });
        }

        // Drop journal entries above the rewind point before recounting kept
        // staged bytes.
        let keep_count = keep_last.as_u64();
        assert!(
            u64::try_from(self.index_journal.len()).is_ok_and(|n| n >= keep_count),
            "rewind point must be journaled"
        );
        while u64::try_from(self.index_journal.len()).is_ok_and(|n| n > keep_count) {
            self.index_journal.pop();
        }

        // Drop staged-but-unsynced bytes for discarded frames. Kept pending
        // bytes end at the rewind point when the rewind stays in the pending
        // batch; otherwise the batch is fully replaced.
        if new_seg == self.active_segment_id && new_offset >= self.pending_write_start_offset {
            let mut kept_bytes = 0usize;
            let mut kept_frames = 0usize;
            for placement in &self.index_journal {
                if placement.offset >= self.pending_write_start_offset {
                    kept_bytes += placement.frame_len;
                    kept_frames += 1;
                }
            }
            self.pending_write_buffer.truncate(kept_bytes);
            self.pending_frame_lens.truncate(kept_frames);
        } else {
            self.pending_write_buffer.clear();
            self.pending_frame_lens.clear();
            self.pending_write_start_offset = new_offset;
        }

        self.active_segment_id = new_seg;
        self.active_segment_file_id = FileId::wal_segment(new_seg);
        self.active_first_index = self.seg_first_index(new_seg);
        self.active_header_crc = self.seg_header_crcs[&new_seg];
        self.segment_write_offset = new_offset;
        self.last_record_crc = new_crc;
        self.last_entry_index = keep_last;
        self.last_entry_term = if keep_last.as_u64() == 0 {
            Term::from_raw(0)
        } else {
            self.journal_at(keep_last).term
        };
    }

    /// Returns the journaled placement for an index. Journal positions mirror
    /// log indices starting at 1, so the lookup is a direct offset.
    fn journal_at(&self, index: LogIndex) -> &FramePlacement {
        let pos = index.as_u64().saturating_sub(1);
        assert!(
            u64::try_from(self.index_journal.len()).is_ok_and(|n| pos < n),
            "index must be journaled"
        );
        #[allow(clippy::cast_possible_truncation)]
        let idx = pos as usize;
        &self.index_journal[idx]
    }

    /// Returns the first index of a live segment.
    fn seg_first_index(&self, seg_id: u64) -> LogIndex {
        if seg_id == self.active_segment_id {
            return self.active_first_index;
        }
        self.sealed_segments
            .iter()
            .find(|(id, _, _)| *id == seg_id)
            .map_or(LogIndex::from_raw(1), |(_, first, _)| *first)
    }

    fn rotate_segment(&mut self, output: &mut EngineOutput) {
        // Flush any pending data in previous segment first
        if !self.pending_write_buffer.is_empty() {
            self.flush_pending_batch(output);
        }

        if self.last_entry_index >= self.active_first_index {
            self.sealed_segments.push((
                self.active_segment_id,
                self.active_first_index,
                self.last_entry_index,
            ));
        }

        self.active_segment_id += 1;
        self.active_segment_file_id = FileId::wal_segment(self.active_segment_id);

        let create_op_id = self.next_op_id();
        output.device_ops.push(DeviceOp::Create {
            op_id: create_op_id,
            file_id: self.active_segment_file_id,
            preallocate_bytes: self.config.segment_capacity_bytes,
        });

        if self.config.sync_directories {
            let sync_dir_op_id = self.next_op_id();
            output.device_ops.push(DeviceOp::SyncDir {
                op_id: sync_dir_op_id,
            });
        }

        let sector_size = self.config.sector_size;
        let header = WalSegmentHeader::new(
            sector_size,
            self.active_segment_id,
            self.last_entry_index
                .next()
                .unwrap_or(self.last_entry_index),
            self.last_entry_term,
        );

        let mut header_buf = vec![0u8; sector_size.as_usize()];
        assert!(
            header.encode(&mut header_buf).is_ok(),
            "segment header buffer is sector sized"
        );

        let write_hdr_op_id = self.next_op_id();
        output.device_ops.push(DeviceOp::Write {
            op_id: write_hdr_op_id,
            file_id: self.active_segment_file_id,
            offset: 0,
            payload: Bytes::from(header_buf),
        });

        self.segment_write_offset = sector_size.as_u64();
        self.last_record_crc = header.header_crc;
        self.pending_write_start_offset = self.segment_write_offset;
        self.active_first_index = self
            .last_entry_index
            .next()
            .unwrap_or(self.last_entry_index);
        self.active_header_crc = header.header_crc;
        self.seg_header_crcs
            .insert(self.active_segment_id, header.header_crc);
    }

    fn flush_pending_batch(&mut self, output: &mut EngineOutput) {
        // Callers guarantee a non-empty batch: staged appends always carry
        // payload bytes or tokens, so an empty flush would only emit a useless sync.
        if !self.pending_write_buffer.is_empty() {
            let write_op_id = self.next_op_id();
            let batch_bytes = Bytes::copy_from_slice(&self.pending_write_buffer);
            output.device_ops.push(DeviceOp::Write {
                op_id: write_op_id,
                file_id: self.active_segment_file_id,
                offset: self.pending_write_start_offset,
                payload: batch_bytes,
            });
            self.pending_write_buffer.clear();
            self.pending_frame_lens.clear();
            self.pending_write_start_offset = self.segment_write_offset;
        }

        let sync_op_id = self.next_op_id();
        output.device_ops.push(DeviceOp::Fdatasync {
            op_id: sync_op_id,
            file_id: self.active_segment_file_id,
        });

        self.in_flight_wal_syncs
            .push_back((sync_op_id, std::mem::take(&mut self.pending_tokens)));
    }

    fn handle_completion(&mut self, completion: &DeviceCompletion, output: &mut EngineOutput) {
        if let Err(e) = &completion.result {
            self.fatal_error = Some(e.clone());
            return;
        }

        if let Some(pos) = self
            .hard_state_syncs
            .iter()
            .position(|entry| entry.sync_op_id == completion.op_id)
        {
            let sync = self.hard_state_syncs.remove(pos);
            let read_op_id = self.next_op_id();
            output.device_ops.push(DeviceOp::Read {
                op_id: read_op_id,
                file_id: FileId::HARD_STATE,
                offset: (u64::from(sync.slot)) * self.config.sector_size.as_u64(),
                len: self.config.sector_size.as_usize(),
            });
            self.hard_state_verifies.push(HardStateVerify {
                read_op_id,
                token: sync.token,
                slot: sync.slot,
                sequence_no: sync.sequence_no,
                term: sync.term,
                vote: sync.vote,
            });
            return;
        }

        if let Some(pos) = self
            .hard_state_verifies
            .iter()
            .position(|entry| entry.read_op_id == completion.op_id)
        {
            let verify = self.hard_state_verifies.remove(pos);
            let verified = match &completion.result {
                Ok(CompletionPayload::Read(bytes)) => {
                    HardStateSlot::decode(bytes, verify.slot, self.config.sector_size).is_ok_and(
                        |slot| {
                            slot.sequence_no == verify.sequence_no
                                && slot.term == verify.term
                                && slot.voted_for == verify.vote
                        },
                    )
                }
                _ => false,
            };
            if verified {
                output.persisted_tokens.push(verify.token);
            } else {
                self.fatal_error = Some(StorageError::DeviceError(
                    "hard-state read-back verification failed".to_string(),
                ));
            }
            return;
        }

        if self
            .in_flight_wal_syncs
            .iter()
            .any(|&(op, _)| op == completion.op_id)
        {
            self.completed_wal_syncs.insert(completion.op_id);
            while let Some(&(front_op, _)) = self.in_flight_wal_syncs.front() {
                if !self.completed_wal_syncs.remove(&front_op) {
                    break;
                }
                if let Some((_, tokens)) = self.in_flight_wal_syncs.pop_front() {
                    output.persisted_tokens.extend(tokens);
                }
                if !self.pending_write_buffer.is_empty() || !self.pending_tokens.is_empty() {
                    self.flush_pending_batch(output);
                }
            }
        }
    }

    /// Pure sans-IO crash recovery scanner.
    ///
    /// Scans alternating hard-state slots and WAL segments sequentially, chaining CRC32Cs
    /// and classifying the log state as `Clean`, `TornTail`, or `MidLogCorruption`.
    /// Every valid frame's byte-exact placement is recorded so the engine can
    /// later rewind a conflicting suffix without re-reading the disk.
    pub fn scan_recovery(
        hard_state_bytes: &[u8],
        segments: &[(WalSegmentHeader, &[u8])],
        sector_size: SectorSize,
    ) -> Result<RecoveredStorageState, StorageError> {
        let hard_state_outcome = recover_hard_state(hard_state_bytes, sector_size)?;
        let (current_term, voted_for) = match &hard_state_outcome {
            HardStateRecoveryOutcome::BothValid {
                term, voted_for, ..
            }
            | HardStateRecoveryOutcome::OneSlotCorrupted {
                term, voted_for, ..
            } => (*term, *voted_for),
        };

        let mut entries = Vec::new();
        let mut placements = Vec::new();
        let mut last_index = LogIndex::from_raw(0);
        let mut last_term = Term::from_raw(0);
        let mut classification = LogRecoveryClassification::Clean;

        let sector_len = sector_size.as_usize();

        for (seg_idx, (seg_hdr, seg_bytes)) in segments.iter().enumerate() {
            let mut offset = sector_len;
            let mut prev_crc = seg_hdr.header_crc;
            let total_len = seg_bytes.len();

            while offset < total_len {
                let Some(slice) = seg_bytes.get(offset..) else {
                    break;
                };
                let scan_res = decode_wal_frame(
                    slice,
                    sector_size,
                    prev_crc,
                    seg_hdr.segment_id,
                    offset as u64,
                );

                match scan_res {
                    WalScanResult::Valid {
                        frame,
                        bytes_consumed,
                        next_crc,
                    } => {
                        last_index = frame.index;
                        last_term = frame.term;
                        placements.push(FramePlacement {
                            index: frame.index,
                            seg_id: seg_hdr.segment_id,
                            offset: offset as u64,
                            crc_before: prev_crc,
                            crc_after: next_crc,
                            term: frame.term,
                            frame_len: bytes_consumed,
                        });
                        prev_crc = next_crc;
                        entries.push(frame.to_entry());
                        offset += bytes_consumed;
                    }
                    WalScanResult::Unallocated => {
                        break;
                    }
                    WalScanResult::Corrupted(_) => {
                        let mut has_valid_after = has_valid_records_ahead(
                            seg_bytes.get(offset + sector_len..).unwrap_or(&[]),
                            sector_size,
                        );
                        if !has_valid_after {
                            for (_, later_bytes) in segments.iter().skip(seg_idx + 1) {
                                if has_valid_records_ahead(later_bytes, sector_size) {
                                    has_valid_after = true;
                                    break;
                                }
                            }
                        }

                        if has_valid_after {
                            classification = LogRecoveryClassification::MidLogCorruption {
                                corrupt_index: last_index.next().unwrap_or(last_index),
                                corrupt_offset: offset as u64,
                            };
                        } else {
                            classification = LogRecoveryClassification::TornTail {
                                truncated_offset: offset as u64,
                                last_valid_index: last_index,
                            };
                        }
                        break;
                    }
                }
            }

            if classification != LogRecoveryClassification::Clean {
                break;
            }
        }

        Ok(RecoveredStorageState {
            current_term,
            voted_for,
            hard_state_outcome,
            log: RecoveredLog {
                entries,
                last_index,
                last_term,
                classification,
                placements,
            },
        })
    }
}

/// Scans sectors following a corrupt record for any structurally valid record header.
///
/// A structurally valid header (non-zero index, known kind, sane length) is treated
/// as evidence of mid-log corruption, which is the conservative classification: the
/// node is sidelined for repair instead of truncating data that might be committed.
/// Term 0 is accepted because leaders append no-op entries at term 0 on election.
fn has_valid_records_ahead(buf: &[u8], sector_size: SectorSize) -> bool {
    let sector_len = sector_size.as_usize();
    let mut cur = 0;

    while cur + sector_len <= buf.len() {
        let slice = &buf[cur..];
        if !slice[..sector_len].iter().all(|&b| b == 0)
            && slice.len() >= WAL_RECORD_HEADER_FIXED_LEN
        {
            let raw_index = u64::from_le_bytes([
                slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
            ]);
            let payload_len =
                u32::from_le_bytes([slice[16], slice[17], slice[18], slice[19]]) as usize;
            let kind_byte = slice[20];

            if raw_index > 0
                && (kind_byte == 0 || kind_byte == 1)
                && payload_len <= 16 * 1024 * 1024
            {
                return true;
            }
        }
        cur += sector_len;
    }

    false
}
