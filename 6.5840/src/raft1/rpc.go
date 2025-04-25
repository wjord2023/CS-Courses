package raft

import "6.5840/raftapi"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).

	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.candidateLogUptodate(args) {
		reply.VoteGranted = true
		rf.writeVotedFor(args.CandidateId)
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) candidateLogUptodate(args *RequestVoteArgs) bool {
	if rf.LastLogTerm() < args.LastLogTerm {
		return true
	} else if rf.LastLogTerm() > args.LastLogTerm {
		return false
	} else {
		return rf.LastLogIndex() <= args.LastLogIndex
	}
}

type AppendEntriesArgs struct {
	Term         int // leader’s term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones

	PrevLogTerm int   // term of prevLogIndex entry
	Entries     []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)

	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.hearedHeartBeat = true
	rf.leaderId = args.LeaderId
	reply.Success = true
	reply.Term = rf.currentTerm

	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1

	//  Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	} else if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.convertToFollower(args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.LastLogIndex() {
		reply.Success = false
		reply.XLen = len(rf.logs)
		return
	}

	term := rf.logs[rf.GetRelativeIndex(args.PrevLogIndex)].Term
	if term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = term
		for i, log := range rf.logs {
			if log.Term == term {
				reply.XIndex = i
				break
			}
		}
		reply.XLen = len(rf.logs)
		return
	}

	if len(args.Entries) != 0 {
		rf.logs = rf.logs[:rf.GetRelativeIndex(args.PrevLogIndex+1)]
		rf.appendLog(args.Entries)
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := rf.LastLogIndex()
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		DPrintf(rf.state, "server %v update commit index to %v", rf.me, rf.commitIndex)
		rf.mu.Unlock()
		rf.applyCommand(rf.applyCh)
		rf.mu.Lock()
	}
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw bytes of snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// Save snapshot file, discard any existing or partial snapshot
	// with a smaller index
	if rf.LastLogIndex() >= args.LastIncludedIndex && rf.logs[args.LastIncludedIndex].Term == args.LastIncludedTerm {
		rf.Snapshot(args.LastIncludedIndex, args.Data)
		return
	} else {
		rf.Snapshot(rf.LastLogIndex(), args.Data)
	}

	applyMsg := raftapi.ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
	rf.applyCh <- applyMsg
}
