# ADR 0018: Week 4 KV Store, Sessions, Reads, Checker, and Workload

## Status
Accepted

## Context
Week 4 needs a replicated KV store with sessions, three read paths, a
trusted linearizability checker, and a workload that ties faults to
histories. The checker must be validated, not just written.

## Options Considered
1. Value-based CAS in both store and checker: simple but contradicts the
   thesis-style session design where versions are log indexes.
2. Index-based versions everywhere with tombstones: reads of deleted keys
   report the delete index, the checker syncs versions from observed
   responses, and snapshots carry tombstones.
3. Skip the checker and rely on the online invariant checker: rejected.
   Invariants cannot judge client-observed linearizability.

## Decision
Option 2. `KvStore` keeps per-key versions equal to the last write's log
index, tombstones preserve delete versions, and RegisterSession binds the
session id to its log index. The checker models registers and lists with
opaque versions taken from observed responses, composes by key, and falls
back to a whole-store model for bounded range histories. CAS compares
expected versions against current versions on both sides.

## Consequences
- A delete of a never-written key records version 0 until it commits; the
  workload waits for the tombstone so histories stay accurate.
- Histories containing Range run register-only so the whole-store model
  stays sound; mixed list-plus-range runs are dedicated future work.
- The Porcupine JSON export ships, but the Go cross-check runs as a nightly
  job where Go is installed.
