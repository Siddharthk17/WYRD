#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_possible_truncation)]

//! `anvil-node`: integration glue owning `RaftCore` and the `KvStore`.
//!
//! The node is sans-IO over environment traits: callers drive ticks,
//! deliver messages, submit persistence operations to a `Device`, and feed
//! completions back. The simulator and the production server share this code.

use anvil_kv::{KvCommand, KvConfig, KvResponse, KvStore};
use anvil_raft::{Input, Output, PersistOp, RaftConfig, RaftCore, ReadIndexResult, Role};
use anvil_types::{LogIndex, NodeId};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;

/// Read consistency levels for client reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Linearizable read through the leader heartbeat round.
    Linearizable,
    /// Linearizable read issued on a follower.
    FollowerLinearizable,
    /// Local lease read. Only safe under the configured drift bound.
    Lease,
    /// Explicitly non-linearizable stale read, excluded from checking.
    Stale,
}

/// Node behavior switches.
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeConfig {
    /// State machine limits.
    pub kv: KvConfig,
    /// Whether lease reads are permitted. Off by default.
    pub lease_enabled: bool,
    /// Lease duration in ticks. Must be shorter than the minimum election
    /// timeout minus the drift bound when enabled.
    pub lease_ticks: u64,
    /// Maximum clock drift bound in ticks for lease safety.
    pub drift_bound_ticks: u64,
    /// Canary switch for Week 4 validation: skip the heartbeat round and
    /// answer leader reads immediately. The checker must catch the stale
    /// read. Never enabled in production.
    pub skip_read_confirmation: bool,
}

/// A client write request. `RegisterSession` carries no session yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeWrite {
    RegisterSession,
    Put {
        session: anvil_types::SessionId,
        seq: anvil_types::SeqNo,
        acked: anvil_types::SeqNo,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        session: anvil_types::SessionId,
        seq: anvil_types::SeqNo,
        acked: anvil_types::SeqNo,
        key: Bytes,
    },
    Cas {
        session: anvil_types::SessionId,
        seq: anvil_types::SeqNo,
        acked: anvil_types::SeqNo,
        key: Bytes,
        expected_version: LogIndex,
        value: Bytes,
    },
    Append {
        session: anvil_types::SessionId,
        seq: anvil_types::SeqNo,
        acked: anvil_types::SeqNo,
        key: Bytes,
        value: Bytes,
    },
}

/// A client read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRead {
    Get {
        key: Bytes,
    },
    List {
        key: Bytes,
    },
    Range {
        start: Bytes,
        end: Bytes,
        limit: usize,
    },
}

/// Client-visible response with the serving read index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientResponse {
    /// Response payload.
    pub response: KvResponse,
    /// Raft log index the read was served at.
    pub read_index: LogIndex,
}

/// Effects emitted by one node step for the environment to execute.
#[derive(Debug, Default)]
pub struct NodeOutput {
    /// Raft messages to the network.
    pub messages: Vec<anvil_raft::OutboundMessage>,
    /// Persistence operations for the storage engine.
    pub persist_ops: Vec<PersistOp>,
    /// Completed client writes in completion order.
    pub client_responses: Vec<ClientResponse>,
    /// Completed client reads in completion order.
    pub read_responses: Vec<ClientResponse>,
}

/// Pending linearizable read waiting for quorum confirmation or apply.
#[derive(Debug, Clone)]
struct PendingRead {
    read: NodeRead,
    consistency: ReadConsistency,
    read_index: Option<LogIndex>,
    context: Option<Bytes>,
}

/// The shared node: Raft core plus applied state machine.
#[allow(clippy::struct_field_names)]
pub struct Node {
    id: NodeId,
    raft: RaftCore,
    kv: KvStore,
    config: NodeConfig,
    raft_output: Output,
    pending_reads: Vec<PendingRead>,
    read_seq: u64,
    applied: LogIndex,
    last_leader_contact: u64,
    now_tick: u64,
}

impl Node {
    /// Creates a node from recovered state and an isolated RNG stream.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        raft_config: RaftConfig,
        recovered: anvil_raft::RecoveredState,
        node_config: NodeConfig,
        rng: ChaCha8Rng,
    ) -> Self {
        Self {
            id: node_id,
            raft: RaftCore::new(raft_config, recovered, rng),
            kv: KvStore::new(node_config.kv),
            config: node_config,
            raft_output: Output::new(),
            pending_reads: Vec::new(),
            read_seq: 0,
            applied: LogIndex::from_raw(0),
            last_leader_contact: 0,
            now_tick: 0,
        }
    }

    /// Returns the node id.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.id
    }

    /// Returns the Raft role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.raft.role()
    }

    /// Returns the commit index.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.raft.commit_index()
    }

    /// Returns the applied index.
    #[must_use]
    pub const fn applied_index(&self) -> LogIndex {
        self.applied
    }

    /// Returns the rolling state hash.
    #[must_use]
    pub fn state_hash(&self) -> [u8; 32] {
        self.kv.state_hash()
    }

    /// Returns the term.
    #[must_use]
    pub fn term(&self) -> anvil_types::Term {
        self.raft.term()
    }

    /// Advances the logical clock.
    pub fn tick(&mut self, output: &mut NodeOutput) {
        self.now_tick = self.now_tick.saturating_add(1);
        self.raft_output.clear();
        self.raft.step(Input::Tick, &mut self.raft_output);
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
    }

    /// Delivers a Raft message.
    pub fn receive(&mut self, from: NodeId, message: anvil_raft::Message, output: &mut NodeOutput) {
        self.last_leader_contact = self.now_tick;
        self.raft_output.clear();
        self.raft
            .step(Input::Recv { from, message }, &mut self.raft_output);
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
    }

    /// Submits a client write to the leader. Returns false when not leader.
    pub fn client_write(&mut self, write: NodeWrite, output: &mut NodeOutput) -> bool {
        if self.raft.role() != Role::Leader {
            return false;
        }
        let mut buf = Vec::new();
        let cmd = match write {
            NodeWrite::RegisterSession => KvCommand::RegisterSession,
            NodeWrite::Put {
                session,
                seq,
                acked,
                key,
                value,
            } => KvCommand::Put {
                session,
                seq,
                acked,
                key,
                value,
            },
            NodeWrite::Delete {
                session,
                seq,
                acked,
                key,
            } => KvCommand::Delete {
                session,
                seq,
                acked,
                key,
            },
            NodeWrite::Cas {
                session,
                seq,
                acked,
                key,
                expected_version,
                value,
            } => KvCommand::Cas {
                session,
                seq,
                acked,
                key,
                expected_version,
                value,
            },
            NodeWrite::Append {
                session,
                seq,
                acked,
                key,
                value,
            } => KvCommand::Append {
                session,
                seq,
                acked,
                key,
                value,
            },
        };
        cmd.encode(&mut buf);
        self.raft_output.clear();
        self.raft.step(
            Input::Propose {
                payload: Bytes::from(buf),
            },
            &mut self.raft_output,
        );
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
        true
    }

    /// Serves a client read at the requested consistency level.
    pub fn client_read(
        &mut self,
        read: NodeRead,
        consistency: ReadConsistency,
        output: &mut NodeOutput,
    ) -> Option<ClientResponse> {
        match consistency {
            ReadConsistency::Stale => Some(ClientResponse {
                response: self.eval_read(&read),
                read_index: self.applied,
            }),
            ReadConsistency::Lease => self.read_lease(&read),
            ReadConsistency::Linearizable => self.read_leader(&read, output),
            ReadConsistency::FollowerLinearizable => {
                self.pending_reads.push(PendingRead {
                    read,
                    consistency,
                    read_index: Some(self.raft.commit_index()),
                    context: None,
                });
                self.serve_ready_reads(output);
                None
            }
        }
    }

    /// Returns the commit index a follower should wait for. The simulator
    /// supplies the leader's commit index; the server resolves it through
    /// the same call after contacting the leader.
    pub fn follower_read_target(&self) -> LogIndex {
        self.raft.commit_index()
    }

    /// Notifies the storage layer that a persist operation completed.
    pub fn on_persisted(&mut self, token: anvil_raft::PersistToken, output: &mut NodeOutput) {
        self.raft_output.clear();
        self.raft
            .step(Input::Persisted { token }, &mut self.raft_output);
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
    }

    /// Proposes a virtual-time tick. Leaders call this periodically so time
    /// enters the log deterministically.
    pub fn propose_clock_tick(&mut self, tick: u64, output: &mut NodeOutput) -> bool {
        if self.raft.role() != Role::Leader {
            return false;
        }
        let mut buf = Vec::new();
        KvCommand::ClockTick { tick }.encode(&mut buf);
        self.raft_output.clear();
        self.raft.step(
            Input::Propose {
                payload: Bytes::from(buf),
            },
            &mut self.raft_output,
        );
        self.drain_raft_output(output);
        true
    }

    /// Directly applies a committed entry payload, for tests that bypass Raft.
    pub fn apply_entry_for_test(&mut self, index: LogIndex, payload: &[u8]) -> KvResponse {
        self.kv.apply(index, payload).unwrap_or(KvResponse::Ok)
    }

    fn read_lease(&mut self, read: &NodeRead) -> Option<ClientResponse> {
        if !self.config.lease_enabled {
            return None;
        }
        if self.raft.role() != Role::Leader {
            return None;
        }
        let since_contact = self.now_tick.saturating_sub(self.last_leader_contact);
        if since_contact > self.config.lease_ticks {
            return None;
        }
        Some(ClientResponse {
            response: self.eval_read(read),
            read_index: self.applied,
        })
    }

    fn read_leader(&mut self, read: &NodeRead, output: &mut NodeOutput) -> Option<ClientResponse> {
        if self.raft.role() != Role::Leader {
            return None;
        }
        if self.config.skip_read_confirmation {
            return Some(ClientResponse {
                response: self.eval_read(read),
                read_index: self.applied,
            });
        }
        self.read_seq = self.read_seq.wrapping_add(1);
        let mut ctx = Vec::new();
        ctx.extend_from_slice(&self.read_seq.to_le_bytes());
        ctx.extend_from_slice(&self.now_tick.to_le_bytes());
        let context = Bytes::from(ctx);
        self.pending_reads.push(PendingRead {
            read: read.clone(),
            consistency: ReadConsistency::Linearizable,
            read_index: None,
            context: Some(context.clone()),
        });
        self.raft_output.clear();
        self.raft
            .step(Input::ReadIndex { context }, &mut self.raft_output);
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
        None
    }

    fn eval_read(&self, read: &NodeRead) -> KvResponse {
        match read {
            NodeRead::Get { key } => self.kv.read_register(key),
            NodeRead::List { key } => self.kv.read_list(key),
            NodeRead::Range { start, end, limit } => self.kv.read_range(start, end, *limit),
        }
    }

    fn serve_ready_reads(&mut self, output: &mut NodeOutput) {
        let mut i = 0;
        while i < self.pending_reads.len() {
            let ready = match self.pending_reads[i].consistency {
                ReadConsistency::Linearizable => false,
                ReadConsistency::FollowerLinearizable => {
                    let target = self.pending_reads[i].read_index.unwrap_or(self.applied);
                    self.applied >= target
                }
                ReadConsistency::Lease | ReadConsistency::Stale => true,
            };
            if ready {
                let pending = self.pending_reads.remove(i);
                let response = self.eval_read(&pending.read);
                output.read_responses.push(ClientResponse {
                    response,
                    read_index: self.applied,
                });
            } else {
                i += 1;
            }
        }
    }

    fn drain_raft_output(&mut self, output: &mut NodeOutput) {
        output.messages.append(&mut self.raft_output.messages);
        output.persist_ops.append(&mut self.raft_output.persist_ops);
        let committed = std::mem::take(&mut self.raft_output.committed_entries);
        for entry in committed {
            if entry.kind == anvil_types::EntryKind::NoOp {
                self.applied = entry.index;
                let mut nested = Output::new();
                self.raft
                    .step(Input::Applied { index: entry.index }, &mut nested);
                output.messages.extend(nested.messages);
                output.persist_ops.extend(nested.persist_ops);
                continue;
            }
            let index = entry.index;
            let response = self
                .kv
                .apply(index, &entry.payload)
                .unwrap_or(KvResponse::Ok);
            self.applied = index;
            output.client_responses.push(ClientResponse {
                response,
                read_index: index,
            });
            let mut nested = Output::new();
            self.raft.step(Input::Applied { index }, &mut nested);
            output.messages.extend(nested.messages);
            output.persist_ops.extend(nested.persist_ops);
        }
        let completed = std::mem::take(&mut self.raft_output.completed_reads);
        for item in &completed {
            self.serve_confirmed_read(item, output);
        }
        self.raft_output.clear();
    }

    fn serve_confirmed_read(&mut self, completed: &ReadIndexResult, output: &mut NodeOutput) {
        let mut i = 0;
        while i < self.pending_reads.len() {
            let matches = self.pending_reads[i].context.as_ref() == Some(&completed.context);
            if matches {
                let pending = self.pending_reads.remove(i);
                if self.applied < completed.read_index {
                    self.pending_reads.push(PendingRead {
                        read_index: Some(completed.read_index),
                        ..pending
                    });
                    i += 1;
                    continue;
                }
                let response = self.eval_read(&pending.read);
                output.read_responses.push(ClientResponse {
                    response,
                    read_index: completed.read_index,
                });
            } else {
                i += 1;
            }
        }
    }
}
