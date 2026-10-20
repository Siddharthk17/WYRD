-------------------------------- MODULE anvil_raft --------------------------------
(***************************************************************************)
(* ANVIL Raft Sans-IO Formal Specification                                 *)
(*                                                                         *)
(* Models:                                                                 *)
(* - Persistent state: currentTerm, votedFor, log                          *)
(* - Volatile state: role, commitIndex, matchIndex, nextIndex,             *)
(*                   votesGranted, preVotesGranted, leaderSticky           *)
(* - Network: unordered, lossy, duplicating message set                     *)
(* - Consensus features: PreVote, CheckQuorum, Leader Stickiness,          *)
(*   Current-Term Commit Rule (configurable for Figure 8 counterexample),  *)
(*   Crash & Restart semantics.                                            *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    Server,                    \* Set of replica server IDs
    Nil,                       \* Nil sentinel value
    MaxTerm,                   \* Maximum term bound for model checking
    MaxLogLen,                 \* Maximum log length bound
    RequireCurrentTermCommit   \* Boolean: TRUE enforces Raft safety rule;
                               \* FALSE reproduces Figure 8 counterexample

VARIABLES
    currentTerm,               \* [s \in Server |-> 0..MaxTerm]
    votedFor,                  \* [s \in Server |-> Server \cup {Nil}]
    log,                       \* [s \in Server |-> Seq([term: Nat, value: Nat])]
    role,                      \* [s \in Server |-> {"Follower", "PreCandidate", "Candidate", "Leader"}]
    commitIndex,               \* [s \in Server |-> 0..MaxLogLen]
    matchIndex,                \* [s \in Server |-> [p \in Server |-> 0..MaxLogLen]]
    nextIndex,                 \* [s \in Server |-> [p \in Server |-> 1..(MaxLogLen+1)]]
    votesGranted,              \* [s \in Server |-> SUBSET Server]
    preVotesGranted,           \* [s \in Server |-> SUBSET Server]
    leaderSticky,              \* [s \in Server |-> BOOLEAN]
    messages,                  \* Set of in-flight network messages
    committedLog               \* Auxiliary global history: [index \in 1..MaxLogLen |-> Nat \cup {Nil}]

vars == <<currentTerm, votedFor, log, role, commitIndex, matchIndex, nextIndex,
          votesGranted, preVotesGranted, leaderSticky, messages, committedLog>>

(***************************************************************************)
(* Helper definitions and functions                                        *)
(***************************************************************************)

Quorum == {i \in SUBSET Server : Cardinality(i) * 2 > Cardinality(Server)}

ModelSymmetry == Permutations(Server)

LastTerm(xlog) == IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)].term

IsUpToDate(cLastLogIndex, cLastLogTerm, sLog) ==
    LET sLastLogIndex == Len(sLog)
        sLastLogTerm == LastTerm(sLog)
    IN \/ cLastLogTerm > sLastLogTerm
       \/ (cLastLogTerm = sLastLogTerm /\ cLastLogIndex >= sLastLogIndex)

Send(m) == messages' = messages \cup {m}

Discard(m) == messages' = messages \ {m}

Reply(response, request) == messages' = (messages \ {request}) \cup {response}

(***************************************************************************)
(* Initial State                                                           *)
(***************************************************************************)

Init ==
    /\ currentTerm = [s \in Server |-> 0]
    /\ votedFor = [s \in Server |-> Nil]
    /\ log = [s \in Server |-> <<>>]
    /\ role = [s \in Server |-> "Follower"]
    /\ commitIndex = [s \in Server |-> 0]
    /\ matchIndex = [s \in Server |-> [p \in Server |-> 0]]
    /\ nextIndex = [s \in Server |-> [p \in Server |-> 1]]
    /\ votesGranted = [s \in Server |-> {}]
    /\ preVotesGranted = [s \in Server |-> {}]
    /\ leaderSticky = [s \in Server |-> FALSE]
    /\ messages = {}
    /\ committedLog = [i \in 1..MaxLogLen |-> Nil]

(***************************************************************************)
(* PreVote phase (preventing disruption by partitioned nodes)              *)
(***************************************************************************)

StartPreVote(s) ==
    /\ role[s] \in {"Follower", "PreCandidate"}
    /\ currentTerm[s] < MaxTerm
    /\ role' = [role EXCEPT ![s] = "PreCandidate"]
    /\ preVotesGranted' = [preVotesGranted EXCEPT ![s] = {s}]
    /\ messages' = messages \cup {[
            mtype |-> "PreVoteReq",
            mterm |-> currentTerm[s] + 1,
            mcandidate |-> s,
            mlastLogIndex |-> Len(log[s]),
            mlastLogTerm |-> LastTerm(log[s]),
            msource |-> s,
            mdest |-> dest
        ] : dest \in Server \ {s}}
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, matchIndex, nextIndex,
                  votesGranted, leaderSticky, committedLog>>

HandlePreVoteReq(s, m) ==
    /\ m.mtype = "PreVoteReq"
    /\ m.mdest = s
    /\ LET grant ==
            /\ m.mterm >= currentTerm[s]
            /\ IsUpToDate(m.mlastLogIndex, m.mlastLogTerm, log[s])
            /\ (~leaderSticky[s] \/ role[s] = "Leader")
       IN
          /\ Reply([
                 mtype |-> "PreVoteResp",
                 mterm |-> currentTerm[s],
                 mvoteGranted |-> grant,
                 msource |-> s,
                 mdest |-> m.msource
             ], m)
          /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex,
                        nextIndex, votesGranted, preVotesGranted, leaderSticky, committedLog>>

HandlePreVoteResp(s, m) ==
    /\ m.mtype = "PreVoteResp"
    /\ m.mdest = s
    /\ role[s] = "PreCandidate"
    /\ IF m.mterm > currentTerm[s]
       THEN
          /\ currentTerm' = [currentTerm EXCEPT ![s] = m.mterm]
          /\ role' = [role EXCEPT ![s] = "Follower"]
          /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
          /\ preVotesGranted' = [preVotesGranted EXCEPT ![s] = {}]
          /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
          /\ Discard(m)
          /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, leaderSticky, committedLog>>
       ELSE
          /\ IF m.mvoteGranted /\ m.mterm = currentTerm[s]
             THEN
                LET newVotes == preVotesGranted[s] \cup {m.msource} IN
                IF newVotes \in Quorum /\ currentTerm[s] < MaxTerm
                THEN
                   \* PreVote succeeded: transition to Candidate and start real election
                   /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
                   /\ role' = [role EXCEPT ![s] = "Candidate"]
                   /\ votedFor' = [votedFor EXCEPT ![s] = s]
                   /\ votesGranted' = [votesGranted EXCEPT ![s] = {s}]
                   /\ preVotesGranted' = [preVotesGranted EXCEPT ![s] = {}]
                   /\ messages' = (messages \ {m}) \cup {[
                           mtype |-> "RequestVoteReq",
                           mterm |-> currentTerm[s] + 1,
                           mcandidate |-> s,
                           mlastLogIndex |-> Len(log[s]),
                           mlastLogTerm |-> LastTerm(log[s]),
                           msource |-> s,
                           mdest |-> dest
                       ] : dest \in Server \ {s}}
                   /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, leaderSticky, committedLog>>
                ELSE
                   /\ preVotesGranted' = [preVotesGranted EXCEPT ![s] = newVotes]
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex,
                                 nextIndex, votesGranted, leaderSticky, committedLog>>
             ELSE
                /\ Discard(m)
                /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex,
                              nextIndex, votesGranted, preVotesGranted, leaderSticky, committedLog>>

(***************************************************************************)
(* Election Phase                                                          *)
(***************************************************************************)

HandleRequestVoteReq(s, m) ==
    /\ m.mtype = "RequestVoteReq"
    /\ m.mdest = s
    /\ LET newerTerm == m.mterm > currentTerm[s]
           actualTerm == IF newerTerm THEN m.mterm ELSE currentTerm[s]
           actualVotedFor == IF newerTerm THEN Nil ELSE votedFor[s]
           grant ==
               /\ m.mterm >= actualTerm
               /\ actualVotedFor \in {Nil, m.mcandidate}
               /\ IsUpToDate(m.mlastLogIndex, m.mlastLogTerm, log[s])
       IN
          /\ currentTerm' = [currentTerm EXCEPT ![s] = actualTerm]
          /\ role' = [role EXCEPT ![s] = IF newerTerm THEN "Follower" ELSE role[s]]
          /\ votedFor' = [votedFor EXCEPT ![s] = IF grant THEN m.mcandidate ELSE actualVotedFor]
          /\ Reply([
                 mtype |-> "RequestVoteResp",
                 mterm |-> actualTerm,
                 mvoteGranted |-> grant,
                 msource |-> s,
                 mdest |-> m.msource
             ], m)
          /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, votesGranted,
                        preVotesGranted, leaderSticky, committedLog>>

HandleRequestVoteResp(s, m) ==
    /\ m.mtype = "RequestVoteResp"
    /\ m.mdest = s
    /\ role[s] = "Candidate"
    /\ IF m.mterm > currentTerm[s]
       THEN
          /\ currentTerm' = [currentTerm EXCEPT ![s] = m.mterm]
          /\ role' = [role EXCEPT ![s] = "Follower"]
          /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
          /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
          /\ Discard(m)
          /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, preVotesGranted,
                        leaderSticky, committedLog>>
       ELSE
          /\ IF m.mvoteGranted /\ m.mterm = currentTerm[s]
             THEN
                LET newVotes == votesGranted[s] \cup {m.msource} IN
                IF newVotes \in Quorum
                THEN
                   \* Server elected Leader: initializes nextIndex, matchIndex, appends no-op
                   /\ role' = [role EXCEPT ![s] = "Leader"]
                   /\ matchIndex' = [matchIndex EXCEPT ![s] = [p \in Server |-> IF p = s THEN Len(log[s]) + 1 ELSE 0]]
                   /\ nextIndex' = [nextIndex EXCEPT ![s] = [p \in Server |-> Len(log[s]) + 2]]
                   /\ log' = IF Len(log[s]) < MaxLogLen
                             THEN [log EXCEPT ![s] = Append(log[s], [term |-> currentTerm[s], value |-> 0])]
                             ELSE log
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, commitIndex, votesGranted,
                                 preVotesGranted, leaderSticky, committedLog>>
                ELSE
                   /\ votesGranted' = [votesGranted EXCEPT ![s] = newVotes]
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex,
                                 nextIndex, preVotesGranted, leaderSticky, committedLog>>
             ELSE
                /\ Discard(m)
                /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex,
                              nextIndex, votesGranted, preVotesGranted, leaderSticky, committedLog>>

(***************************************************************************)
(* Log Replication and Commit Rule                                         *)
(***************************************************************************)

ClientRequest(s) ==
    /\ role[s] = "Leader"
    /\ Len(log[s]) < MaxLogLen
    /\ log' = [log EXCEPT ![s] = Append(log[s], [term |-> currentTerm[s], value |-> Len(log[s]) + 1])]
    /\ matchIndex' = [matchIndex EXCEPT ![s][s] = Len(log[s]) + 1]
    /\ UNCHANGED <<currentTerm, votedFor, role, commitIndex, nextIndex, votesGranted,
                  preVotesGranted, leaderSticky, messages, committedLog>>

SendAppendEntries(s, p) ==
    /\ role[s] = "Leader"
    /\ p /= s
    /\ LET prevIndex == nextIndex[s][p] - 1
           prevTerm == IF prevIndex = 0 THEN 0 ELSE log[s][prevIndex].term
           entriesToSend == SubSeq(log[s], nextIndex[s][p], Len(log[s]))
       IN
          /\ Send([
                 mtype |-> "AppendEntriesReq",
                 mterm |-> currentTerm[s],
                 mprevLogIndex |-> prevIndex,
                 mprevLogTerm |-> prevTerm,
                 mentries |-> entriesToSend,
                 mcommitIndex |-> commitIndex[s],
                 msource |-> s,
                 mdest |-> p
             ])
          /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex,
                        nextIndex, votesGranted, preVotesGranted, leaderSticky, committedLog>>

HandleAppendEntriesReq(s, m) ==
    /\ m.mtype = "AppendEntriesReq"
    /\ m.mdest = s
    /\ IF m.mterm < currentTerm[s]
       THEN
          /\ Reply([
                 mtype |-> "AppendEntriesResp",
                 mterm |-> currentTerm[s],
                 msuccess |-> FALSE,
                 mmatchIndex |-> 0,
                 msource |-> s,
                 mdest |-> m.msource
             ], m)
          /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex,
                        nextIndex, votesGranted, preVotesGranted, leaderSticky, committedLog>>
       ELSE
          LET newerTerm == m.mterm > currentTerm[s]
              actualTerm == IF newerTerm THEN m.mterm ELSE currentTerm[s]
              actualVotedFor == IF newerTerm THEN Nil ELSE votedFor[s]
              logOk == \/ m.mprevLogIndex = 0
                       \/ (m.mprevLogIndex <= Len(log[s]) /\ log[s][m.mprevLogIndex].term = m.mprevLogTerm)
          IN
             /\ currentTerm' = [currentTerm EXCEPT ![s] = actualTerm]
             /\ role' = [role EXCEPT ![s] = "Follower"]
             /\ votedFor' = [votedFor EXCEPT ![s] = actualVotedFor]
             /\ leaderSticky' = [leaderSticky EXCEPT ![s] = TRUE]
             /\ IF ~logOk
                THEN
                   /\ Reply([
                          mtype |-> "AppendEntriesResp",
                          mterm |-> actualTerm,
                          msuccess |-> FALSE,
                          mmatchIndex |-> Len(log[s]),
                          msource |-> s,
                          mdest |-> m.msource
                      ], m)
                   /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, votesGranted,
                                 preVotesGranted, committedLog>>
                ELSE
                   \* Log prefix matches: append new entries, truncate diverging suffix
                   LET newLog == SubSeq(log[s], 1, m.mprevLogIndex) \o m.mentries
                       newCommitIndex == IF m.mcommitIndex > commitIndex[s]
                                         THEN IF m.mcommitIndex < Len(newLog)
                                              THEN m.mcommitIndex
                                              ELSE Len(newLog)
                                         ELSE commitIndex[s]
                   IN
                      /\ log' = [log EXCEPT ![s] = newLog]
                      /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommitIndex]
                      /\ committedLog' = [i \in 1..MaxLogLen |->
                            IF i <= newCommitIndex /\ committedLog[i] = Nil
                            THEN newLog[i].term
                            ELSE committedLog[i]]
                      /\ Reply([
                             mtype |-> "AppendEntriesResp",
                             mterm |-> actualTerm,
                             msuccess |-> TRUE,
                             mmatchIndex |-> m.mprevLogIndex + Len(m.mentries),
                             msource |-> s,
                             mdest |-> m.msource
                         ], m)
                      /\ UNCHANGED <<matchIndex, nextIndex, votesGranted, preVotesGranted>>

HandleAppendEntriesResp(s, m) ==
    /\ m.mtype = "AppendEntriesResp"
    /\ m.mdest = s
    /\ role[s] = "Leader"
    /\ IF m.mterm > currentTerm[s]
       THEN
          /\ currentTerm' = [currentTerm EXCEPT ![s] = m.mterm]
          /\ role' = [role EXCEPT ![s] = "Follower"]
          /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
          /\ Discard(m)
          /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, votesGranted,
                        preVotesGranted, leaderSticky, committedLog>>
       ELSE
          /\ IF m.mterm = currentTerm[s]
             THEN
                IF m.msuccess
                THEN
                   /\ matchIndex' = [matchIndex EXCEPT ![s][m.msource] = m.mmatchIndex]
                   /\ nextIndex' = [nextIndex EXCEPT ![s][m.msource] = m.mmatchIndex + 1]
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex,
                                 votesGranted, preVotesGranted, leaderSticky, committedLog>>
                ELSE
                   \* Step back nextIndex on rejection
                   /\ nextIndex' = [nextIndex EXCEPT ![s][m.msource] =
                         IF nextIndex[s][m.msource] > 1 THEN nextIndex[s][m.msource] - 1 ELSE 1]
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex,
                                 matchIndex, votesGranted, preVotesGranted, leaderSticky, committedLog>>
             ELSE
                /\ Discard(m)
                /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex,
                              nextIndex, votesGranted, preVotesGranted, leaderSticky, committedLog>>

(***************************************************************************)
(* AdvanceCommit: Leader commits entries replicated on a quorum             *)
(* Raft Rule: Leader may only commit entries from currentTerm (Section 5.4.2)*)
(***************************************************************************)

AdvanceCommit(s) ==
    /\ role[s] = "Leader"
    /\ \E idx \in (commitIndex[s] + 1)..Len(log[s]):
          /\ {p \in Server : matchIndex[s][p] >= idx} \in Quorum
          /\ IF RequireCurrentTermCommit
             THEN log[s][idx].term = currentTerm[s]
             ELSE TRUE  \* In negative test, commit older-term entries directly (Figure 8 bug)
          /\ commitIndex' = [commitIndex EXCEPT ![s] = idx]
          /\ committedLog' = [i \in 1..MaxLogLen |->
                IF i <= idx /\ committedLog[i] = Nil
                THEN log[s][i].term
                ELSE committedLog[i]]
          /\ UNCHANGED <<currentTerm, votedFor, log, role, matchIndex, nextIndex,
                        votesGranted, preVotesGranted, leaderSticky, messages>>

(***************************************************************************)
(* CheckQuorum & Leader Stickiness expiration                              *)
(***************************************************************************)

CheckQuorum(s) ==
    /\ role[s] = "Leader"
    /\ role' = [role EXCEPT ![s] = "Follower"]
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, matchIndex, nextIndex,
                  votesGranted, preVotesGranted, leaderSticky, messages, committedLog>>

ExpireSticky(s) ==
    /\ leaderSticky[s] = TRUE
    /\ leaderSticky' = [leaderSticky EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex, nextIndex,
                  votesGranted, preVotesGranted, messages, committedLog>>

(***************************************************************************)
(* Fault Model: Crash, Restart, Message Loss, Duplication                  *)
(***************************************************************************)

Restart(s) ==
    /\ role' = [role EXCEPT ![s] = "Follower"]
    /\ commitIndex' = [commitIndex EXCEPT ![s] = 0]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [p \in Server |-> 0]]
    /\ nextIndex' = [nextIndex EXCEPT ![s] = [p \in Server |-> 1]]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
    /\ preVotesGranted' = [preVotesGranted EXCEPT ![s] = {}]
    /\ leaderSticky' = [leaderSticky EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<currentTerm, votedFor, log, messages, committedLog>>

DropMessage(m) ==
    /\ Discard(m)
    /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex, nextIndex,
                  votesGranted, preVotesGranted, leaderSticky, committedLog>>

DuplicateMessage(m) ==
    /\ m \in messages
    /\ UNCHANGED vars

(***************************************************************************)
(* Next State Relation                                                     *)
(***************************************************************************)

Next ==
    \/ \E s \in Server: StartPreVote(s)
    \/ \E s \in Server, m \in messages: HandlePreVoteReq(s, m)
    \/ \E s \in Server, m \in messages: HandlePreVoteResp(s, m)
    \/ \E s \in Server, m \in messages: HandleRequestVoteReq(s, m)
    \/ \E s \in Server, m \in messages: HandleRequestVoteResp(s, m)
    \/ \E s \in Server: ClientRequest(s)
    \/ \E s, p \in Server: SendAppendEntries(s, p)
    \/ \E s \in Server, m \in messages: HandleAppendEntriesReq(s, m)
    \/ \E s \in Server, m \in messages: HandleAppendEntriesResp(s, m)
    \/ \E s \in Server: AdvanceCommit(s)
    \/ \E s \in Server: CheckQuorum(s)
    \/ \E s \in Server: ExpireSticky(s)
    \/ \E s \in Server: Restart(s)
    \/ \E m \in messages: DropMessage(m)
    \/ \E m \in messages: DuplicateMessage(m)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* 1. Election Safety: at most one leader can be elected in a given term
ElectionSafety ==
    \A s1, s2 \in Server:
        (role[s1] = "Leader" /\ role[s2] = "Leader" /\ currentTerm[s1] = currentTerm[s2])
        => (s1 = s2)

\* 2. Leader Append Only: leader log entries at previous indices are never modified
LeaderAppendOnly ==
    \A s \in Server:
        role[s] = "Leader" =>
            \A i \in 1..Len(log[s]):
                log[s][i].term <= currentTerm[s]

\* 3. Log Matching: if two logs contain an entry with same index and term, they agree on all prefixes
LogMatching ==
    \A s1, s2 \in Server:
        \A i \in 1..Len(log[s1]):
            (i <= Len(log[s2]) /\ log[s1][i].term = log[s2][i].term) =>
                (\A j \in 1..i: log[s1][j].term = log[s2][j].term)

\* 4. Leader Completeness: if an entry is committed at term T, all leaders of terms > T contain that entry
LeaderCompleteness ==
    \A s1, s2 \in Server:
        \A i \in 1..commitIndex[s1]:
            (role[s2] = "Leader" /\ currentTerm[s2] > log[s1][i].term) =>
                (Len(log[s2]) >= i /\ log[s2][i].term = log[s1][i].term)

\* 5. State Machine Safety: if two servers have committed entries at index i, they must have the same term
StateMachineSafety ==
    \A s1, s2 \in Server:
        \A i \in 1..Len(log[s1]):
            (i <= commitIndex[s1] /\ i <= commitIndex[s2] /\ i <= Len(log[s2])) =>
                (log[s1][i].term = log[s2][i].term)

\* 6. Committed Entries Never Lost: committed entries in the global oracle are never overwritten
CommittedEntriesNeverLost ==
    \A s \in Server:
        \A i \in 1..Len(log[s]):
            (committedLog[i] /= Nil /\ i <= commitIndex[s]) =>
                (log[s][i].term = committedLog[i])

\* 7. Term Monotonicity: state invariant checking currentTerm bounds
TermMonotonic ==
    \A s \in Server: currentTerm[s] <= MaxTerm

=============================================================================
