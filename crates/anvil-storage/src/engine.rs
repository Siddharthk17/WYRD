#![forbid(unsafe_code)]

//! Sans-IO Storage Engine with natural batching group commit and crash recovery.

use crate::config::StorageConfig;
use crate::device::{DeviceCompletion, DeviceOp, FileId, SectorSize};
use crate::error::StorageError;
use crate::format::hard_state::{HardStateRecoveryOutcome, HardStateSlot, recover_hard_state};
use crate::format::wal::{
    WAL_RECORD_HEADER_FIXED_LEN, WalRecordFrame, WalScanResult, WalSegmentHeader, decode_wal_frame,
};
use crate::recovery::{LogRecoveryClassification, RecoveredLog, RecoveredStorageState};
use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;

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
    /// Request log compaction / snapshot retention up to `compact_index`.
    CompactLog {
        /// Boundary up to which earlier log records may be purged.
        compact_index: LogIndex,
    },
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
    wal_sync_in_flight: bool,
    in_flight_wal_sync_op_id: Option<u64>,
    in_flight_wal_tokens: Vec<u64>,

    // In-flight hard state sync operations (op_id, token)
    hard_state_sync_tokens: Vec<(u64, u64)>,

    // Pending coalesced batch (while sync is in flight)
    pending_tokens: Vec<u64>,
    pending_write_buffer: Vec<u8>,
    pending_write_start_offset: u64,

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

        Self {
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
            wal_sync_in_flight: false,
            in_flight_wal_sync_op_id: None,
            in_flight_wal_tokens: Vec::with_capacity(64),
            hard_state_sync_tokens: Vec::with_capacity(4),
            pending_tokens: Vec::with_capacity(64),
            pending_write_buffer: Vec::with_capacity(64 * 1024),
            pending_write_start_offset: segment_write_offset,
            fatal_error: None,
        }
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
            wal_sync_in_flight: false,
            in_flight_wal_sync_op_id: None,
            in_flight_wal_tokens: Vec::with_capacity(64),
            hard_state_sync_tokens: Vec::with_capacity(4),
            pending_tokens: Vec::with_capacity(64),
            pending_write_buffer: Vec::with_capacity(64 * 1024),
            pending_write_start_offset: sector_size.as_u64(),
            fatal_error: None,
        }
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
            EngineInput::CompactLog { compact_index } => {
                self.handle_compact_log(compact_index, output);
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
        let _ = slot.encode(sector_size, &mut slot_buf);

        let target_offset = (target_slot as u64) * sector_size.as_u64();
        let write_op_id = self.next_op_id();
        output.device_ops.push(DeviceOp::Write {
            op_id: write_op_id,
            file_id: FileId::HARD_STATE,
            offset: target_offset,
            data: Bytes::from(slot_buf),
        });

        self.active_hard_state_slot = target_slot;

        let sync_op_id = self.next_op_id();
        output.device_ops.push(DeviceOp::Fdatasync {
            op_id: sync_op_id,
            file_id: FileId::HARD_STATE,
        });
        self.hard_state_sync_tokens.push((sync_op_id, token));
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
            if self.wal_sync_in_flight {
                self.pending_tokens.push(token);
            } else {
                output.persisted_tokens.push(token);
            }
            return;
        }

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
            self.last_record_crc = frame.crc32c;
            self.segment_write_offset += frame_len as u64;
            self.last_entry_index = entry.index;
            self.last_entry_term = entry.term;
        }

        self.pending_tokens.push(token);

        // Natural batching: if no sync is in flight, flush immediately
        if !self.wal_sync_in_flight {
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
        let target_prev_index = from_index.checked_sub(1).unwrap_or(LogIndex::from_raw(0));
        self.last_entry_index = target_prev_index;

        // If from_index is <= first entry of current batch, reset pending batch
        self.pending_write_buffer.clear();
        self.pending_tokens.clear();

        self.handle_append_entries(target_prev_index, entries, token, output);
    }

    fn rotate_segment(&mut self, output: &mut EngineOutput) {
        // Flush any pending data in previous segment first
        if !self.pending_write_buffer.is_empty() {
            self.flush_pending_batch(output);
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
        let _ = header.encode(&mut header_buf);

        let write_hdr_op_id = self.next_op_id();
        output.device_ops.push(DeviceOp::Write {
            op_id: write_hdr_op_id,
            file_id: self.active_segment_file_id,
            offset: 0,
            data: Bytes::from(header_buf),
        });

        self.segment_write_offset = sector_size.as_u64();
        self.last_record_crc = header.header_crc;
        self.pending_write_start_offset = self.segment_write_offset;
    }

    fn flush_pending_batch(&mut self, output: &mut EngineOutput) {
        if self.pending_write_buffer.is_empty() && self.pending_tokens.is_empty() {
            return;
        }

        if !self.pending_write_buffer.is_empty() {
            let write_op_id = self.next_op_id();
            let batch_bytes = Bytes::copy_from_slice(&self.pending_write_buffer);
            output.device_ops.push(DeviceOp::Write {
                op_id: write_op_id,
                file_id: self.active_segment_file_id,
                offset: self.pending_write_start_offset,
                data: batch_bytes,
            });
            self.pending_write_buffer.clear();
        }

        let sync_op_id = self.next_op_id();
        output.device_ops.push(DeviceOp::Fdatasync {
            op_id: sync_op_id,
            file_id: self.active_segment_file_id,
        });

        self.wal_sync_in_flight = true;
        self.in_flight_wal_sync_op_id = Some(sync_op_id);
        self.in_flight_wal_tokens.append(&mut self.pending_tokens);
    }

    fn handle_completion(&mut self, completion: &DeviceCompletion, output: &mut EngineOutput) {
        // Enforce ADR 0013: Fail-Fast Crash on Any Sync Error, NEVER retry!
        assert!(
            completion.result.is_ok(),
            "FATAL: storage sync operation failed under ADR 0013 (no retry)"
        );

        // 1. Check if completion is for a HardState sync
        if let Some(pos) = self
            .hard_state_sync_tokens
            .iter()
            .position(|&(op, _)| op == completion.op_id)
        {
            let (_, token) = self.hard_state_sync_tokens.remove(pos);
            output.persisted_tokens.push(token);
            return;
        }

        // 2. Check if completion is for active WAL fdatasync
        if Some(completion.op_id) == self.in_flight_wal_sync_op_id {
            output
                .persisted_tokens
                .append(&mut self.in_flight_wal_tokens);
            self.wal_sync_in_flight = false;
            self.in_flight_wal_sync_op_id = None;

            // Coalesced natural batching: flush any writes that queued while sync was in flight
            if !self.pending_write_buffer.is_empty() || !self.pending_tokens.is_empty() {
                self.flush_pending_batch(output);
            }
        }
    }

    fn handle_compact_log(&mut self, compact_index: LogIndex, _output: &mut EngineOutput) {
        let _ = (self.config.segment_capacity_bytes, compact_index);
    }

    /// Pure sans-IO crash recovery scanner.
    ///
    /// Scans alternating hard-state slots and WAL segments sequentially, chaining CRC32Cs
    /// and classifying the log state as `Clean`, `TornTail`, or `MidLogCorruption`.
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
        let mut last_index = LogIndex::from_raw(0);
        let mut last_term = Term::from_raw(0);
        let mut classification = LogRecoveryClassification::Clean;

        let sector_len = sector_size.as_usize();

        for (seg_hdr, seg_bytes) in segments {
            let mut offset = sector_len;
            let mut prev_crc = seg_hdr.header_crc;
            let total_len = seg_bytes.len();

            while offset < total_len {
                let slice = &seg_bytes[offset..];
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
                        prev_crc = next_crc;
                        entries.push(frame.to_entry());
                        offset += bytes_consumed;
                    }
                    WalScanResult::Unallocated => {
                        // All subsequent bytes in this sector are zeroes: reached end of log
                        break;
                    }
                    WalScanResult::Corrupted(_) => {
                        // Check whether corruption is at the tail or mid-log
                        let remainder = &seg_bytes[offset + sector_len..];
                        let has_valid_after = has_valid_records_ahead(
                            remainder,
                            sector_size,
                            seg_hdr.segment_id,
                            (offset + sector_len) as u64,
                        );

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
            },
        })
    }
}

/// Helper function to determine if any valid record exists ahead of a corrupted sector.
fn has_valid_records_ahead(
    buf: &[u8],
    sector_size: SectorSize,
    _segment_id: u64,
    _offset: u64,
) -> bool {
    let sector_len = sector_size.as_usize();
    let mut cur = 0;

    while cur + sector_len <= buf.len() {
        let slice = &buf[cur..];
        // If the sector is not completely zeroes, inspect record header structure
        if !slice[..sector_len].iter().all(|&b| b == 0)
            && slice.len() >= WAL_RECORD_HEADER_FIXED_LEN
        {
            let raw_index = u64::from_le_bytes([
                slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
            ]);
            let raw_term = u64::from_le_bytes([
                slice[8], slice[9], slice[10], slice[11], slice[12], slice[13], slice[14],
                slice[15],
            ]);
            let payload_len =
                u32::from_le_bytes([slice[16], slice[17], slice[18], slice[19]]) as usize;
            let kind_byte = slice[20];

            if raw_index > 0
                && raw_term > 0
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
