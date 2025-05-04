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
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if rf.candidateLogUptodate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.electionTimerReset()
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) candidateLogUptodate(args *RequestVoteArgs) bool {
	if rf.log.LastTerm() < args.LastLogTerm {
		return true
	} else if rf.log.LastTerm() > args.LastLogTerm {
		return false
	} else {
		return rf.log.LastIndex() <= args.LastLogIndex
	}
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.LastIndex(),
		LastLogTerm:  rf.log.LastTerm(),
	}
	return args
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

type AppendEntriesArgs struct {
	Term         int // leader’s term
	PrevLogIndex int // index of log entry immediately preceding new ones

	PrevLogTerm int       // term of prevLogIndex entry
	Entries     []Entries // log entries to store (empty for heartbeat; may send more than one for efficiency)

	LeaderCommit int // leader’s commitIndex
}

func (rf *Raft) genAppendEntriesArgs(server int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		PrevLogIndex: rf.PrevLogIndex(server),
		PrevLogTerm:  rf.PrevLogTerm(server),
		LeaderCommit: rf.commitIndex,
	}

	args.Entries = make([]Entries, 0)
	args.Entries = append(args.Entries, rf.log.GetSlice(args.PrevLogIndex+1, rf.log.LastIndex()+1)...)

	return args
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	Confict bool
	XTerm   int // term in the conflicting entry (if any)
	XIndex  int // index of first entry with that term (if any)
	XLen    int // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Confict = false

	//  Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	} else if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.convertToFollower(args.Term)
	}

	rf.electionTimerReset()

	if args.PrevLogIndex < rf.log.FirstIndex() {
		rf.DPrintf("prev log index %d < first index %d\n", args.PrevLogIndex, rf.log.FirstIndex())
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.log.LastIndex() {
		reply.Confict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.LastIndex() + 1
		return
	}

	term := rf.log.Get(args.PrevLogIndex).Term
	if term != args.PrevLogTerm {
		reply.Confict = true
		reply.XTerm = term
		index := rf.log.FirstIndex()
		for i := args.PrevLogIndex - 1 - rf.log.FirstIndex(); i >= rf.log.FirstIndex(); i-- {
			if rf.log[i].Term != term {
				index = i + 1 + rf.log.FirstIndex()
				break
			}
		}
		reply.XIndex = index
		reply.XLen = rf.log.LastIndex() + 1
		return
	}

	// rf.log = append(rf.log.GetSlice(rf.log.FirstIndex(), args.PrevLogIndex+1), args.Entries...)
	start := args.PrevLogIndex + 1 - rf.log.FirstIndex()
	for i, e := range args.Entries {
		if start+i >= len(rf.log) || rf.log[start+i].Term != e.Term {
			rf.log = append(rf.log[:start+i], args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := rf.log.LastIndex()
		newCommitIndex := min(args.LeaderCommit, lastNewEntryIndex)
		if rf.commitIndex < newCommitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
			rf.DPrintf("update commit index to %d\n", rf.commitIndex)
		}
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LastIncludedIndex int    // snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw bytes of snapshot chunk, starting at offset
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.log.FirstIndex(),
		LastIncludedTerm:  rf.log.FirstTerm(),
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
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

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	} else if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.convertToFollower(args.Term)
	}

	rf.electionTimerReset()

	if rf.commitIndex >= args.LastIncludedIndex {
		return
	}

	rf.RenewLog(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persister.Save(rf.encodeState(), args.Data)

	rf.commitIndex = rf.log.FirstIndex()
	rf.DPrintf("update commitIndex to %d\n", rf.commitIndex)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
