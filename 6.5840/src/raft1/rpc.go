package raft

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

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.convertToFollower(args.Term)
		rf.mu.Lock()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.candidateLogUptodate(args) {
		DPrintf("server %d vote for %d", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) candidateLogUptodate(args *RequestVoteArgs) bool {
	if rf.LastLogTerm() < args.LastLogTerm {
		return true
	} else if rf.LastLogTerm() > args.LastLogTerm {
		return false
	} else {
		return rf.LastLogTerm() <= args.LastLogIndex
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("server %d receive AppendEntries from %d, args: %+v", rf.me, args.LeaderId, args)
	rf.mu.Lock()
	defer func() {
		if args.LeaderCommit > rf.commitIndex {
			DPrintf("leadercommit %d lastlogindex %d logs %v", args.LeaderCommit, rf.LastLogIndex(), rf.logs)
			rf.commitIndex = min(args.LeaderCommit, rf.LastLogIndex())
			DPrintf("follow %d update commitIndex to %d", rf.me, rf.commitIndex)
		}
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	reply.Success = true

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.convertToFollower(args.Term)
		rf.mu.Lock()
	}

	rf.hearedHeartBeat = true
	rf.leaderId = args.LeaderId

	if args.PrevLogIndex > rf.LastLogIndex() {
		reply.Success = false
		DPrintf("server %d receive AppendEntries from %d, args: %+v, but prevlogindex %d > lastlogindex %d", rf.me, args.LeaderId, args, args.PrevLogIndex, rf.LastLogIndex())
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		DPrintf("server %d receive AppendEntries from %d, args: %+v, but prevlogterm %d != %d", rf.me, args.LeaderId, args, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	if args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
	}
}
