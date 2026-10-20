#![forbid(unsafe_code)]
#![allow(clippy::cast_possible_truncation)]

//! In-memory log representation, slicing, and cryptographic digest calculation.

use anvil_types::{Entry, LogIndex, Term};

/// In-memory Raft log entries storage with incremental BLAKE3 digest tracking.
#[derive(Debug, Clone)]
pub struct RaftLog {
    entries: Vec<Entry>,
    hasher: blake3::Hasher,
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
        let kind_byte = match entry.kind {
            anvil_types::EntryKind::Normal => 0u8,
            anvil_types::EntryKind::NoOp => 1u8,
        };
        hasher.update(&[kind_byte]);
        hasher.update(&(entry.payload.len() as u64).to_le_bytes());
        hasher.update(&entry.payload);
    }

    /// Constructs a `RaftLog` from pre-existing recovered entries.
    #[must_use]
    pub fn new(entries: Vec<Entry>) -> Self {
        let mut hasher = blake3::Hasher::new();
        for e in &entries {
            Self::hash_entry(&mut hasher, e);
        }
        Self { entries, hasher }
    }

    /// Returns the total number of entries in the log.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the log contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the highest log index present in the log, or 0 if empty.
    #[must_use]
    pub fn last_index(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(LogIndex::from_raw(0), |e| e.index)
    }

    /// Returns the term of the highest log index, or 0 if empty.
    #[must_use]
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(Term::from_raw(0), |e| e.term)
    }

    /// Returns the term of the entry at `index`, or `Some(Term(0))` if `index == 0`.
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index.is_zero() {
            return Some(Term::from_raw(0));
        }
        let raw = index.as_u64();
        if raw == 0 || raw > self.entries.len() as u64 {
            return None;
        }
        self.entries.get((raw - 1) as usize).map(|e| e.term)
    }

    /// Returns a reference to the entry at `index`.
    #[must_use]
    pub fn entry_at(&self, index: LogIndex) -> Option<&Entry> {
        let raw = index.as_u64();
        if raw == 0 || raw > self.entries.len() as u64 {
            return None;
        }
        self.entries.get((raw - 1) as usize)
    }

    /// Appends a new entry to the end of the log and incrementally updates digest.
    pub fn append(&mut self, entry: Entry) {
        Self::hash_entry(&mut self.hasher, &entry);
        self.entries.push(entry);
    }

    /// Truncates diverging suffix starting at `from_index` and appends `new_entries`.
    pub fn truncate_and_append(&mut self, from_index: LogIndex, new_entries: Vec<Entry>) {
        let raw = from_index.as_u64();
        let target_len = (raw.saturating_sub(1)) as usize;
        let did_truncate = target_len < self.entries.len();
        if did_truncate {
            self.entries.truncate(target_len);
            let mut hasher = blake3::Hasher::new();
            for e in &self.entries {
                Self::hash_entry(&mut hasher, e);
            }
            self.hasher = hasher;
        }
        for e in new_entries {
            Self::hash_entry(&mut self.hasher, &e);
            self.entries.push(e);
        }
    }

    /// Slices entries starting from `from_index` up to `max_bytes` total payload size.
    #[must_use]
    pub fn slice_from(&self, from_index: LogIndex, max_bytes: usize) -> Vec<Entry> {
        let raw = from_index.as_u64();
        if raw == 0 || raw > self.entries.len() as u64 {
            return Vec::new();
        }
        let start_idx = (raw - 1) as usize;
        let mut result = Vec::new();
        let mut accum_bytes = 0usize;

        for entry in &self.entries[start_idx..] {
            let entry_size = entry.payload.len() + 32; // payload + header estimate
            if !result.is_empty() && accum_bytes + entry_size > max_bytes {
                break;
            }
            result.push(entry.clone());
            accum_bytes += entry_size;
        }

        result
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

    /// Returns the incremental BLAKE3 cryptographic digest of all entries in the log.
    #[must_use]
    pub fn compute_digest(&self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }

    /// Returns a slice over all in-memory entries.
    #[must_use]
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}
