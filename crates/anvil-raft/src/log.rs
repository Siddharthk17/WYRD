#![forbid(unsafe_code)]
#![allow(clippy::cast_possible_truncation)]

//! In-memory log representation, slicing, and cryptographic digest calculation.
//!
//! The log covers `(base_index, last_index]`. Prefix compaction drops entries
//! at or below the snapshot index minus the retained window, only after the
//! snapshot is durable, and never beyond the applied index. Compacted entries
//! report no term: the leader must send a snapshot (Week 5, deliverable 2).

use anvil_types::{Entry, LogIndex, Term};

/// In-memory Raft log entries storage with incremental BLAKE3 digest tracking.
#[derive(Debug, Clone)]
pub struct RaftLog {
    entries: Vec<Entry>,
    hasher: blake3::Hasher,
    base_index: LogIndex,
    base_term: Term,
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl RaftLog {
    fn hash_entry(hasher: &mut blake3::Hasher, entry: &Entry) {
        hasher.update(&entry.index.as_u64().to_le_bytes());
        hasher.update(&entry.term.as_u64().to_le_bytes());
        hasher.update(&[entry.kind.as_u8()]);
        hasher.update(&(entry.payload.len() as u64).to_le_bytes());
        hasher.update(&entry.payload);
    }

    fn rebuild_hasher(&mut self) {
        let mut hasher = blake3::Hasher::new();
        for existing in &self.entries {
            Self::hash_entry(&mut hasher, existing);
        }
        self.hasher = hasher;
    }

    /// Constructs a `RaftLog` from pre-existing recovered entries.
    #[must_use]
    pub fn new(entries: Vec<Entry>) -> Self {
        let base_index = entries.first().map_or(LogIndex::from_raw(0), |entry| {
            LogIndex::from_raw(entry.index.as_u64().saturating_sub(1))
        });
        let base_raw = base_index.as_u64();
        for (offset, entry) in entries.iter().enumerate() {
            let expected = base_raw + offset as u64 + 1;
            assert!(
                entry.index.as_u64() == expected,
                "recovered log must be contiguous"
            );
        }
        let mut hasher = blake3::Hasher::new();
        for entry in &entries {
            Self::hash_entry(&mut hasher, entry);
        }
        let base_term = Term::from_raw(0);
        Self {
            entries,
            hasher,
            base_index,
            base_term,
        }
    }

    /// Returns the total number of entries held in memory.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the log holds no entries in memory.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the highest compacted index, 0 when nothing is compacted.
    #[must_use]
    pub const fn base_index(&self) -> LogIndex {
        self.base_index
    }

    /// Returns the term of the highest compacted index, 0 when nothing is compacted.
    #[must_use]
    pub const fn base_term(&self) -> Term {
        self.base_term
    }

    /// Returns the highest log index present in the log, or the base when empty.
    #[must_use]
    pub fn last_index(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(self.base_index, |entry| entry.index)
    }

    /// Returns the term of the highest log index, or 0 if empty.
    #[must_use]
    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(Term::from_raw(0), |entry| entry.term)
    }

    /// Returns the term of the entry at `index`, or `Some(Term(0))` if `index == 0`.
    /// Returns `None` for compacted or future indexes.
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index.is_zero() {
            return Some(Term::from_raw(0));
        }
        let raw = index.as_u64();
        let base = self.base_index.as_u64();
        if raw == base {
            return Some(self.base_term);
        }
        if raw < base {
            return None;
        }
        let offset = raw - base - 1;
        if offset > self.entries.len() as u64 {
            return None;
        }
        self.entries.get(offset as usize).map(|entry| entry.term)
    }

    /// Returns a reference to the entry at `index`.
    #[must_use]
    pub fn entry_at(&self, index: LogIndex) -> Option<&Entry> {
        let raw = index.as_u64();
        let base = self.base_index.as_u64();
        if raw == 0 || raw <= base {
            return None;
        }
        let offset = raw - base - 1;
        if offset > self.entries.len() as u64 {
            return None;
        }
        self.entries.get(offset as usize)
    }

    /// Appends a new entry to the end of the log and incrementally updates digest.
    pub fn append(&mut self, entry: Entry) {
        Self::hash_entry(&mut self.hasher, &entry);
        self.entries.push(entry);
    }

    /// Truncates diverging suffix starting at `from_index` and appends `new_entries`.
    ///
    /// A prefix at or below the compacted base is already snapshotted, so the
    /// overlapping head of `new_entries` is dropped instead of rejected.
    pub fn truncate_and_append(&mut self, from_index: LogIndex, new_entries: Vec<Entry>) {
        let raw = from_index.as_u64();
        let base = self.base_index.as_u64();
        if raw <= base {
            let skip = (base + 1).saturating_sub(raw) as usize;
            if skip >= new_entries.len() {
                return;
            }
            let remaining: Vec<Entry> = new_entries.into_iter().skip(skip).collect();
            let resume = LogIndex::from_raw(base + 1);
            self.truncate_and_append(resume, remaining);
            return;
        }
        let target_len = (raw - base - 1) as usize;
        let did_truncate = target_len < self.entries.len();
        if did_truncate {
            self.entries.truncate(target_len);
            self.rebuild_hasher();
        }
        for entry in new_entries {
            Self::hash_entry(&mut self.hasher, &entry);
            self.entries.push(entry);
        }
    }

    /// Drops the log prefix at or below `up_to`, which must be a durable
    /// snapshot index minus the retained window, and never beyond the applied
    /// index. Callers enforce durability; this enforces the index bound.
    pub fn compact(&mut self, up_to: LogIndex) {
        assert!(
            self.term_at(up_to).is_some(),
            "compaction target must have a known term"
        );
        let up_to_term = self.term_at(up_to).unwrap_or(Term::from_raw(0));
        self.compact_with_term(up_to, up_to_term);
    }

    /// Drops the log prefix at or below `up_to`, recording its term so that
    /// later `AppendEntries` previous-entry checks still resolve. Callers
    /// enforce durability and the never-beyond-applied bound.
    pub fn compact_with_term(&mut self, up_to: LogIndex, up_to_term: Term) {
        let raw = up_to.as_u64();
        let base = self.base_index.as_u64();
        let last = self.last_index().as_u64();
        assert!(raw > base, "compaction must advance the base index");
        assert!(raw <= last, "compaction must not pass the last index");
        let drop = (raw - base) as usize;
        self.entries.drain(..drop);
        self.base_index = up_to;
        self.base_term = up_to_term;
        self.rebuild_hasher();
    }

    /// Atomically installs a verified snapshot: the log is replaced by the
    /// snapshot's last included index and term. All earlier entries are gone,
    /// so the caller must have persisted the snapshot state first.
    pub fn install_snapshot(&mut self, last_included_index: LogIndex, last_included_term: Term) {
        self.entries.clear();
        self.base_index = last_included_index;
        self.base_term = last_included_term;
        self.rebuild_hasher();
    }

    /// Slices entries starting from `from_index` up to `max_bytes` total payload size.
    /// A start at or below the compacted base begins at the first live entry;
    /// callers that need the compacted prefix must use a snapshot.
    #[must_use]
    pub fn slice_from(&self, from_index: LogIndex, max_bytes: usize) -> Vec<Entry> {
        let raw = from_index.as_u64();
        let base = self.base_index.as_u64();
        let start_raw = raw.max(base + 1);
        let offset = start_raw - base - 1;
        if offset > self.entries.len() as u64 {
            return Vec::new();
        }
        let start_idx = offset as usize;
        let mut sliced = Vec::new();
        let mut accum_bytes = 0usize;

        for entry in &self.entries[start_idx..] {
            let entry_size = entry.payload.len() + 32; // payload + header estimate
            if !sliced.is_empty() && accum_bytes + entry_size > max_bytes {
                break;
            }
            sliced.push(entry.clone());
            accum_bytes += entry_size;
        }

        sliced
    }

    /// Finds the first index in the log having the specified `term`.
    #[must_use]
    pub fn find_first_index_of_term(&self, term: Term) -> Option<LogIndex> {
        for entry in &self.entries {
            if entry.term == term {
                return Some(entry.index);
            }
        }
        None
    }

    /// Finds the last index in the log having the specified `term`.
    #[must_use]
    pub fn find_last_index_of_term(&self, term: Term) -> Option<LogIndex> {
        for entry in self.entries.iter().rev() {
            if entry.term == term {
                return Some(entry.index);
            }
        }
        None
    }

    /// Returns the incremental BLAKE3 cryptographic digest of entries in memory.
    #[must_use]
    pub fn compute_digest(&self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }

    /// Returns a slice over in-memory entries after the compacted prefix.
    #[must_use]
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}
