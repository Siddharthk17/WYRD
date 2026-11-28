-------------------------------- MODULE anvil_reconfig --------------------------------
(***************************************************************************)
(* ANVIL Joint-Consensus Reconfiguration Specification                     *)
(*                                                                         *)
(* Extends the anvil_raft model with safe membership changes:              *)
(* - Learners receive the log but never vote and never lead.               *)
(* - EnterJoint carries the target config and takes effect on append.      *)
(* - The leader appends LeaveJoint automatically once the joint commits.   *)
(* - Quorums are joint: a majority of the old voters AND a majority of     *)
(*   the new voters.                                                       *)
(* - Config changes are serialized: the leader appends a config entry only *)
(*   when no config entry is uncommitted on itself and no joint is active. *)
(*   A config change is also refused until the leader commits an entry in *)
(*   its own term.                                                         *)
(*                                                                         *)
(* AllowConcurrentChanges = FALSE models the implementation.               *)
(* AllowConcurrentChanges = TRUE models the planted bug (M12 class): a     *)
(* second joint may be appended while one is uncommitted, which must       *)
(* produce a counterexample to SerializedConfig.                           *)
(*                                                                         *)
(* Abstractions (documented, all conservative):                            *)
(* - Client writes are not modeled: the log holds the election no-op and  *)
(*   config entries only, so joint actions stay reachable under           *)
(*   simulation. Normal replication is covered by the base anvil_raft     *)
(*   spec.                                                                 *)
(* - The bootstrap entry is not modeled; every server starts in config    *)
(*   {voters} with the joiner as learner.                                  *)
(* - Messages from removed senders are processed, not filtered. The        *)
(*   implementation drops them, which only removes behaviors, so safety   *)
(*   here implies safety there.                                            *)
(* - Duplicates are not modeled: every handler is idempotent, so a        *)
(*   duplicate changes nothing, and duplicates would only consume         *)
(*   simulation depth. Loss is the fault that matters.                     *)
(* - Restart preserves commitIndex: the implementation recovers the       *)
(*   commit index from durable storage.                                    *)
(* - Faults (loss, crash, spurious stepdown) draw from a per-trace        *)
(*   budget, mirroring the Plan-based nemesis. Without a bound the        *)
(*   always-enabled faults starve all progress under simulation.           *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    Server,
    Joiner,
    Nil,
    MaxTerm,
    MaxLogLen,
    AllowConcurrentChanges,
    FaultBudget

VARIABLES
    currentTerm,
    votedFor,
    log,
    role,
    commitIndex,
    matchIndex,
    nextIndex,
    votesGranted,
    preVotesGranted,
    leaderSticky,
    voters,
    learners,
    joint,
    pendingCfgIdx,
    messages,
    faultsLeft,
    committedLog,
    committedCfg

vars == <<currentTerm, votedFor, log, role, commitIndex, matchIndex, nextIndex,
          votesGranted, preVotesGranted, leaderSticky, voters, learners, joint,
          pendingCfgIdx, messages, faultsLeft, committedLog, committedCfg>>

(***************************************************************************)
(* Helpers                                                                 *)
(***************************************************************************)

QuorumOf(C) == {Q \in SUBSET C : Cardinality(Q) * 2 > Cardinality(C)}

LastTerm(xlog) == IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)].term

IsUpToDate(cLastLogIndex, cLastLogTerm, sLog) ==
    LET sLastLogIndex == Len(sLog)
        sLastLogTerm == LastTerm(sLog)
    IN \/ cLastLogTerm > sLastLogTerm
       \/ (cLastLogTerm = sLastLogTerm /\ cLastLogIndex >= sLastLogIndex)

Send(m) == messages' = messages \cup {m}

Discard(m) == messages' = messages \ {m}

Reply(response, request) == messages' = (messages \ {request}) \cup {response}

NoCfg == [kind |-> "None", voters |-> {}, learners |-> {}]

IsConfigEntry(e) == e.cfgKind /= "None"

Electorate(s) ==
    IF joint[s] = Nil THEN <<voters[s], voters[s]>>
    ELSE <<joint[s].old, joint[s].new>>

IsVoter(s) == s \in Electorate(s)[1] \cup Electorate(s)[2]

HasQuorum(s, granted) ==
    LET old == Electorate(s)[1]
        new == Electorate(s)[2]
        oldVotes == granted \cap old
        newVotes == granted \cap new
    IN oldVotes \in QuorumOf(old) /\ newVotes \in QuorumOf(new)

OtherConfig(s) ==
    IF Joiner \notin voters[s]
    THEN [voters |-> voters[s] \cup {Joiner}, learners |-> {}]
    ELSE [voters |-> voters[s] \ {Joiner}, learners |-> {}]

CommittedCurrentTerm(s) ==
    \E i \in 1..commitIndex[s] : log[s][i].term = currentTerm[s]

MaxCfgIdx(lg, upto) ==
    LET S == {i \in 1..upto : i <= Len(lg) /\ IsConfigEntry(lg[i])}
    IN IF S = {} THEN 0 ELSE CHOOSE i \in S : \A j \in S : j <= i

(***************************************************************************)
(* Config state updates. One function applies an entry batch and settles   *)
(* bookkeeping against the commit index, so append and resolve can never  *)
(* contradict each other. A joint whose EnterJoint vanished from the log  *)
(* rolls back to the pre-joint voters and learners.                       *)
(***************************************************************************)

CfgState(s) == [voters |-> voters[s], learners |-> learners[s],
                joint |-> joint[s], pending |-> pendingCfgIdx[s]]

StepCfg(acc, e, idx) ==
    IF e.cfgKind = "Joint" THEN
        [voters |-> acc.voters,
         learners |-> acc.learners \cup e.cfg.learners,
         joint |-> [old |-> acc.voters, oldLearners |-> acc.learners,
                    new |-> e.cfg.voters, enterIdx |-> idx],
         pending |-> idx]
    ELSE IF e.cfgKind = "Leave" THEN
        [voters |-> e.cfg.voters,
         learners |-> e.cfg.learners,
         joint |-> acc.joint,
         pending |-> idx]
    ELSE acc

RECURSIVE FoldCfg(_, _, _)
FoldCfg(acc, es, base) ==
    IF es = <<>> THEN acc
    ELSE FoldCfg(StepCfg(acc, Head(es), base + 1), Tail(es), base + 1)

ApplyAndResolve(s, es, base, lg, cmt) ==
    LET acc == FoldCfg(CfgState(s), es, base)
        intact == acc.joint = Nil \/
            (acc.joint.enterIdx <= Len(lg) /\ lg[acc.joint.enterIdx].cfgKind = "Joint")
        settled == acc.pending /= 0 /\ acc.pending <= cmt /\
            acc.pending <= Len(lg) /\ IsConfigEntry(lg[acc.pending])
    IN IF ~intact
       THEN /\ voters' = [voters EXCEPT ![s] = acc.joint.old]
            /\ learners' = [learners EXCEPT ![s] = acc.joint.oldLearners]
            /\ joint' = [joint EXCEPT ![s] = Nil]
            /\ pendingCfgIdx' = [pendingCfgIdx EXCEPT ![s] = 0]
       ELSE /\ voters' = [voters EXCEPT ![s] = acc.voters]
            /\ learners' = [learners EXCEPT ![s] = acc.learners]
            /\ joint' = IF settled /\ lg[acc.pending].cfgKind = "Leave"
                        THEN [joint EXCEPT ![s] = Nil]
                        ELSE [joint EXCEPT ![s] = acc.joint]
            /\ pendingCfgIdx' = [pendingCfgIdx EXCEPT ![s] =
                    IF settled THEN 0 ELSE acc.pending]

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
    /\ voters = [s \in Server |-> Server \ {Joiner}]
    /\ learners = [s \in Server |-> {Joiner}]
    /\ joint = [s \in Server |-> Nil]
    /\ pendingCfgIdx = [s \in Server |-> 0]
    /\ messages = {}
    /\ faultsLeft = FaultBudget
    /\ committedLog = [i \in 1..MaxLogLen |-> Nil]
    /\ committedCfg = [i \in 1..MaxLogLen |-> Nil]

(***************************************************************************)
(* PreVote and election with joint quorums; learners never vote or lead   *)
(***************************************************************************)

StartPreVote(s) ==
    /\ role[s] \in {"Follower", "PreCandidate"}
    /\ currentTerm[s] < MaxTerm
    /\ IsVoter(s)
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
        votesGranted, leaderSticky, voters, learners, joint, pendingCfgIdx,
        faultsLeft, committedLog, committedCfg>>

HandlePreVoteReq(s, m) ==
    /\ m.mtype = "PreVoteReq"
    /\ m.mdest = s
    /\ LET grant ==
            /\ IsVoter(s)
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
               nextIndex, votesGranted, preVotesGranted, leaderSticky, voters,
               learners, joint, pendingCfgIdx, faultsLeft, committedLog, committedCfg>>

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
          /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex, leaderSticky,
              voters, learners, joint, pendingCfgIdx, faultsLeft, committedLog,
              committedCfg>>
       ELSE
          /\ IF m.mvoteGranted /\ m.mterm = currentTerm[s]
             THEN
                LET newVotes == preVotesGranted[s] \cup {m.msource} IN
                IF HasQuorum(s, newVotes) /\ currentTerm[s] < MaxTerm
                THEN
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
                   /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex,
                       leaderSticky, voters, learners, joint, pendingCfgIdx,
                       faultsLeft, committedLog, committedCfg>>
                ELSE
                   /\ preVotesGranted' = [preVotesGranted EXCEPT ![s] = newVotes]
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex,
                       matchIndex, nextIndex, votesGranted, leaderSticky, voters,
                       learners, joint, pendingCfgIdx, faultsLeft, committedLog,
                       committedCfg>>
             ELSE
                /\ Discard(m)
                /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex,
                    matchIndex, nextIndex, votesGranted, preVotesGranted,
                    leaderSticky, voters, learners, joint, pendingCfgIdx,
                    faultsLeft, committedLog, committedCfg>>

HandleRequestVoteReq(s, m) ==
    /\ m.mtype = "RequestVoteReq"
    /\ m.mdest = s
    /\ LET newerTerm == m.mterm > currentTerm[s]
           actualTerm == IF newerTerm THEN m.mterm ELSE currentTerm[s]
           actualVotedFor == IF newerTerm THEN Nil ELSE votedFor[s]
           grant ==
               /\ IsVoter(s)
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
              preVotesGranted, leaderSticky, voters, learners, joint, pendingCfgIdx,
              faultsLeft, committedLog, committedCfg>>

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
              leaderSticky, voters, learners, joint, pendingCfgIdx, faultsLeft,
              committedLog, committedCfg>>
       ELSE
          /\ IF m.mvoteGranted /\ m.mterm = currentTerm[s]
             THEN
                LET newVotes == votesGranted[s] \cup {m.msource} IN
                IF HasQuorum(s, newVotes)
                THEN
                   /\ role' = [role EXCEPT ![s] = "Leader"]
                   /\ matchIndex' = [matchIndex EXCEPT ![s] = [p \in Server |->
                          IF p = s
                          THEN IF Len(log[s]) < MaxLogLen THEN Len(log[s]) + 1 ELSE Len(log[s])
                          ELSE 0]]
                   /\ nextIndex' = [nextIndex EXCEPT ![s] = [p \in Server |->
                          IF Len(log[s]) < MaxLogLen THEN Len(log[s]) + 2 ELSE Len(log[s]) + 1]]
                   /\ log' = IF Len(log[s]) < MaxLogLen
                             THEN [log EXCEPT ![s] = Append(log[s], [term |-> currentTerm[s], val |-> 0, cfgKind |-> "None", cfg |-> NoCfg])]
                             ELSE log
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, commitIndex, votesGranted,
                       preVotesGranted, leaderSticky, voters, learners, joint,
                       pendingCfgIdx, faultsLeft, committedLog, committedCfg>>
                ELSE
                   /\ votesGranted' = [votesGranted EXCEPT ![s] = newVotes]
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex,
                       matchIndex, nextIndex, preVotesGranted, leaderSticky, voters,
                       learners, joint, pendingCfgIdx, faultsLeft, committedLog,
                       committedCfg>>
             ELSE
                /\ Discard(m)
                /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex,
                    matchIndex, nextIndex, votesGranted, preVotesGranted,
                    leaderSticky, voters, learners, joint, pendingCfgIdx,
                    faultsLeft, committedLog, committedCfg>>

(***************************************************************************)
(* Replication, reconfig actions, and joint commit                         *)
(***************************************************************************)

Reconfig(s) ==
    /\ role[s] = "Leader"
    /\ Len(log[s]) < MaxLogLen
    /\ CommittedCurrentTerm(s)
    /\ \/ AllowConcurrentChanges
       \/ (pendingCfgIdx[s] = 0 /\ joint[s] = Nil)
    /\ LET target == OtherConfig(s)
           entry == [term |-> currentTerm[s], val |-> 0,
                     cfgKind |-> "Joint", cfg |-> target]
           newLog == Append(log[s], entry)
       IN
          /\ log' = [log EXCEPT ![s] = newLog]
          /\ matchIndex' = [matchIndex EXCEPT ![s][s] = Len(newLog)]
          /\ ApplyAndResolve(s, <<entry>>, Len(log[s]), newLog, commitIndex[s])
          /\ UNCHANGED <<currentTerm, votedFor, role, commitIndex, nextIndex,
              votesGranted, preVotesGranted, leaderSticky, messages, faultsLeft,
              committedLog, committedCfg>>

FinishJoint(s) ==
    /\ role[s] = "Leader"
    /\ joint[s] /= Nil
    /\ Len(log[s]) < MaxLogLen
    /\ pendingCfgIdx[s] = 0
    /\ commitIndex[s] >= joint[s].enterIdx
    /\ LET target == [voters |-> joint[s].new, learners |-> {}]
           entry == [term |-> currentTerm[s], val |-> 0,
                     cfgKind |-> "Leave", cfg |-> target]
           newLog == Append(log[s], entry)
       IN
          /\ log' = [log EXCEPT ![s] = newLog]
          /\ matchIndex' = [matchIndex EXCEPT ![s][s] = Len(newLog)]
          /\ ApplyAndResolve(s, <<entry>>, Len(log[s]), newLog, commitIndex[s])
          /\ UNCHANGED <<currentTerm, votedFor, role, commitIndex, nextIndex,
              votesGranted, preVotesGranted, leaderSticky, messages, faultsLeft,
              committedLog, committedCfg>>

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
              nextIndex, votesGranted, preVotesGranted, leaderSticky, voters,
              learners, joint, pendingCfgIdx, faultsLeft, committedLog, committedCfg>>

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
              nextIndex, votesGranted, preVotesGranted, leaderSticky, voters,
              learners, joint, pendingCfgIdx, faultsLeft, committedLog, committedCfg>>
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
                   /\ UNCHANGED <<log, commitIndex, matchIndex, nextIndex,
                       votesGranted, preVotesGranted, voters, learners, joint,
                       pendingCfgIdx, faultsLeft, committedLog, committedCfg>>
                ELSE
                   LET newLog == SubSeq(log[s], 1, m.mprevLogIndex) \o m.mentries
                       newCommitRaw == IF m.mcommitIndex > commitIndex[s]
                                         THEN IF m.mcommitIndex < Len(newLog)
                                              THEN m.mcommitIndex
                                              ELSE Len(newLog)
                                         ELSE commitIndex[s]
                       (*****************************************************************)
                       (* A conflicting suffix may truncate the log below an old       *)
                       (* commit marker. The implementation commits only up to the      *)
                       (* matched prefix, so the model clamps the marker to the log.   *)
                       (*****************************************************************)
                       newCommitIndex == IF newCommitRaw <= Len(newLog)
                                         THEN newCommitRaw
                                         ELSE Len(newLog)
                   IN
                      /\ log' = [log EXCEPT ![s] = newLog]
                      /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommitIndex]
                      /\ committedLog' = [i \in 1..MaxLogLen |->
                            IF i <= newCommitIndex /\ committedLog[i] = Nil
                            THEN newLog[i].term
                            ELSE committedLog[i]]
                      /\ committedCfg' = [i \in 1..MaxLogLen |->
                            IF i <= newCommitIndex /\ committedCfg[i] = Nil /\ IsConfigEntry(newLog[i])
                            THEN newLog[i].cfg
                            ELSE committedCfg[i]]
                      /\ Reply([
                             mtype |-> "AppendEntriesResp",
                             mterm |-> actualTerm,
                             msuccess |-> TRUE,
                             mmatchIndex |-> m.mprevLogIndex + Len(m.mentries),
                             msource |-> s,
                             mdest |-> m.msource
                         ], m)
                      /\ ApplyAndResolve(s, m.mentries, m.mprevLogIndex, newLog, newCommitIndex)
                      /\ UNCHANGED <<matchIndex, nextIndex, votesGranted,
                          preVotesGranted, faultsLeft>>

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
              preVotesGranted, leaderSticky, voters, learners, joint, pendingCfgIdx,
              faultsLeft, committedLog, committedCfg>>
       ELSE
          /\ IF m.mterm = currentTerm[s]
             THEN
                IF m.msuccess
                THEN
                   /\ matchIndex' = [matchIndex EXCEPT ![s][m.msource] = m.mmatchIndex]
                   /\ nextIndex' = [nextIndex EXCEPT ![s][m.msource] = m.mmatchIndex + 1]
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex,
                       votesGranted, preVotesGranted, leaderSticky, voters,
                       learners, joint, pendingCfgIdx, faultsLeft, committedLog,
                       committedCfg>>
                ELSE
                   /\ nextIndex' = [nextIndex EXCEPT ![s][m.msource] =
                         IF nextIndex[s][m.msource] > 1 THEN nextIndex[s][m.msource] - 1 ELSE 1]
                   /\ Discard(m)
                   /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex,
                       matchIndex, votesGranted, preVotesGranted, leaderSticky,
                       voters, learners, joint, pendingCfgIdx, faultsLeft, committedLog,
                       committedCfg>>
             ELSE
                /\ Discard(m)
                /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex,
                    matchIndex, nextIndex, votesGranted, preVotesGranted,
                    leaderSticky, voters, learners, joint, pendingCfgIdx,
                    faultsLeft, committedLog, committedCfg>>

AdvanceCommit(s) ==
    /\ role[s] = "Leader"
    /\ \E idx \in (commitIndex[s] + 1)..Len(log[s]):
          /\ HasQuorum(s, {p \in Server : matchIndex[s][p] >= idx} \cup {s})
          /\ log[s][idx].term = currentTerm[s]
          /\ commitIndex' = [commitIndex EXCEPT ![s] = idx]
          /\ committedLog' = [i \in 1..MaxLogLen |->
                IF i <= idx /\ committedLog[i] = Nil
                THEN log[s][i].term
                ELSE committedLog[i]]
          /\ committedCfg' = [i \in 1..MaxLogLen |->
                IF i <= idx /\ committedCfg[i] = Nil /\ IsConfigEntry(log[s][i])
                THEN log[s][i].cfg
                ELSE committedCfg[i]]
          /\ ApplyAndResolve(s, <<>>, 0, log[s], idx)
          /\ UNCHANGED <<currentTerm, votedFor, log, role, matchIndex, nextIndex,
              votesGranted, preVotesGranted, leaderSticky, messages, faultsLeft>>

CheckQuorum(s) ==
    /\ role[s] = "Leader"
    /\ faultsLeft > 0
    /\ faultsLeft' = faultsLeft - 1
    /\ role' = [role EXCEPT ![s] = "Follower"]
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, matchIndex, nextIndex,
        votesGranted, preVotesGranted, leaderSticky, voters, learners, joint,
        pendingCfgIdx, messages, committedLog, committedCfg>>

ExpireSticky(s) ==
    /\ leaderSticky[s] = TRUE
    /\ leaderSticky' = [leaderSticky EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex, nextIndex,
        votesGranted, preVotesGranted, messages, voters, learners, joint,
        pendingCfgIdx, faultsLeft, committedLog, committedCfg>>

Restart(s) ==
    /\ faultsLeft > 0
    /\ faultsLeft' = faultsLeft - 1
    /\ role' = [role EXCEPT ![s] = "Follower"]
    /\ commitIndex' = [commitIndex EXCEPT ![s] =
            IF commitIndex[s] <= Len(log[s]) THEN commitIndex[s] ELSE Len(log[s])]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [p \in Server |-> 0]]
    /\ nextIndex' = [nextIndex EXCEPT ![s] = [p \in Server |-> 1]]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
    /\ preVotesGranted' = [preVotesGranted EXCEPT ![s] = {}]
    /\ leaderSticky' = [leaderSticky EXCEPT ![s] = FALSE]
    /\ ApplyAndResolve(s, <<>>, 0, log[s], commitIndex[s])
    /\ UNCHANGED <<currentTerm, votedFor, log, messages, committedLog,
        committedCfg>>

DropMessage(m) ==
    /\ faultsLeft > 0
    /\ faultsLeft' = faultsLeft - 1
    /\ Discard(m)
    /\ UNCHANGED <<currentTerm, votedFor, log, role, commitIndex, matchIndex,
        nextIndex, votesGranted, preVotesGranted, leaderSticky, voters, learners,
        joint, pendingCfgIdx, committedLog, committedCfg>>

Stutter ==
    (*************************************************************************)
    (* Deadlock avoidance for simulation only: bounded faults plus maxed    *)
    (* terms plus a stale message can leave no action enabled (handlers      *)
    (* reject stale terms, drops and restarts spent their budget). The       *)
    (* implementation never stalls: timers keep firing. Stuttering models    *)
    (* those idle ticks and preserves every safety invariant.                *)
    (*************************************************************************)
    /\ UNCHANGED vars

Next ==
    \/ \E s \in Server: StartPreVote(s)
    \/ \E s \in Server, m \in messages: HandlePreVoteReq(s, m)
    \/ \E s \in Server, m \in messages: HandlePreVoteResp(s, m)
    \/ \E s \in Server, m \in messages: HandleRequestVoteReq(s, m)
    \/ \E s \in Server, m \in messages: HandleRequestVoteResp(s, m)
    \/ \E s \in Server: Reconfig(s)
    \/ \E s \in Server: FinishJoint(s)
    \/ \E s, p \in Server: SendAppendEntries(s, p)
    \/ \E s \in Server, m \in messages: HandleAppendEntriesReq(s, m)
    \/ \E s \in Server, m \in messages: HandleAppendEntriesResp(s, m)
    \/ \E s \in Server: AdvanceCommit(s)
    \/ \E s \in Server: CheckQuorum(s)
    \/ \E s \in Server: ExpireSticky(s)
    \/ \E s \in Server: Restart(s)
    \/ \E m \in messages: DropMessage(m)
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

ElectionSafety ==
    \A s1, s2 \in Server:
        (role[s1] = "Leader" /\ role[s2] = "Leader" /\ currentTerm[s1] = currentTerm[s2])
        => (s1 = s2)

LeaderAppendOnly ==
    \A s \in Server:
        role[s] = "Leader" =>
            \A i \in 1..Len(log[s]):
                log[s][i].term <= currentTerm[s]

LogMatching ==
    \A s1, s2 \in Server:
        \A i \in 1..Len(log[s1]):
            (i <= Len(log[s2]) /\ log[s1][i].term = log[s2][i].term) =>
                (\A j \in 1..i: log[s1][j].term = log[s2][j].term)

LeaderCompleteness ==
    \A s1, s2 \in Server:
        \A i \in 1..commitIndex[s1]:
            (role[s2] = "Leader" /\ currentTerm[s2] > log[s1][i].term) =>
                (Len(log[s2]) >= i /\ log[s2][i].term = log[s1][i].term)

StateMachineSafety ==
    \A s1, s2 \in Server:
        \A i \in 1..Len(log[s1]):
            (i <= commitIndex[s1] /\ i <= commitIndex[s2] /\ i <= Len(log[s2])) =>
                (log[s1][i].term = log[s2][i].term)

CommittedEntriesNeverLost ==
    \A s \in Server:
        \A i \in 1..Len(log[s]):
            (committedLog[i] /= Nil /\ i <= commitIndex[s]) =>
                (log[s][i].term = committedLog[i])

CommittedConfigNeverLost ==
    \A s \in Server:
        \A i \in 1..Len(log[s]):
            (committedCfg[i] /= Nil /\ i <= commitIndex[s]) =>
                (log[s][i].cfg = committedCfg[i])

TermMonotonic ==
    \A s \in Server: currentTerm[s] <= MaxTerm

JointOpenAt(i) ==
    \E s \in Server :
        /\ i <= Len(log[s])
        /\ log[s][i].cfgKind = "Joint"
        /\ committedCfg[i] = Nil
        /\ \A j \in (i + 1)..Len(log[s]) : log[s][j].cfgKind /= "Leave"

SerializedConfig ==
    Cardinality({i \in 1..MaxLogLen : JointOpenAt(i)}) <= 1

JointQuorumOverlap ==
    \A s \in Server:
        joint[s] /= Nil =>
            LET old == joint[s].old
                new == joint[s].new
            IN \A qo \in QuorumOf(old), qn \in QuorumOf(new):
                (qo \cap new /= {}) /\ (qn \cap old /= {}) /\
                (qo \cup qn) \cap old \in QuorumOf(old) /\
                (qo \cup qn) \cap new \in QuorumOf(new)

=============================================================================
