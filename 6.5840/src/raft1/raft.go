package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	cond      sync.Cond           // Used to signal when a leader is elected
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	state           ServerState
	leaderId        int
	hearedHeartBeat bool
	numVotes        int
}

func (rf *Raft) LastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) LastLogTerm() int {
	return rf.logs[rf.LastLogIndex()].Term
}

func (rf *Raft) PrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) PrevLogTerm(server int) int {
	return rf.logs[rf.PrevLogIndex(server)].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader
	if !isLeader {
		return -1, -1, false
	}

	rf.logs = append(rf.logs, Log{Command: command, Term: rf.currentTerm})
	return rf.LastLogIndex(), rf.currentTerm, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) convertToFollower(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d convert to follower term %d\n", rf.me, rf.currentTerm)
	if rf.state == Follower {
		return
	}
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = term
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d convert to leader term %d\n", rf.me, rf.currentTerm)
	if rf.state == Leader {
		return
	}
	rf.state = Leader
	rf.leaderId = rf.me
	rf.nextIndex = fillSlice(len(rf.peers), rf.LastLogIndex()+1)
	rf.matchIndex = fillSlice(len(rf.peers), 0)
	rf.numVotes = 0
}

func (rf *Raft) convertToCondidate() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	DPrintf("Server %d convert to candidate term %d\n", rf.me, rf.currentTerm)

	// start election
	rf.votedFor = rf.me
	rf.numVotes = 1

	me := rf.me
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.LastLogIndex(), rf.LastLogTerm()}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {

					rf.mu.Unlock()
					rf.convertToFollower(reply.Term)
					rf.mu.Lock()
				}
				if reply.VoteGranted {
					rf.numVotes += 1
					if rf.numVotes >= (len(rf.peers)/2 + 1) {
						rf.mu.Unlock()
						rf.convertToLeader()
						rf.sendHeartBeat()
						rf.mu.Lock()
					}
				}
				rf.mu.Unlock()
			}
		}(server)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != Leader {
			if !rf.hearedHeartBeat {
				DPrintf("Server %d start election\n", rf.me)
				rf.mu.Unlock()
				rf.convertToCondidate()
				rf.mu.Lock()
			}
			rf.hearedHeartBeat = false
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		for server := range rf.peers {
			if server == rf.me {
				continue
			}

			go func(server int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.PrevLogIndex(server), rf.PrevLogTerm(server), []Log{}, rf.commitIndex}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

				// leader should turn into follower if it receives a higher term
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.mu.Unlock()
					rf.convertToFollower(reply.Term)
					rf.mu.Lock()
				}
				rf.mu.Unlock()
			}(server)
		}
	}
}

// If commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine (§5.3)
func (rf *Raft) applyCommand(applyCh chan raftapi.ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		shouldApply := rf.commitIndex > rf.lastApplied
		rf.mu.Unlock()
		if shouldApply {
			rf.mu.Lock()
			rf.lastApplied += 1
			applyMag := raftapi.ApplyMsg{}
			applyMag.CommandValid = true
			applyMag.Command = rf.logs[rf.lastApplied].Command
			applyMag.CommandIndex = rf.lastApplied
			rf.mu.Unlock()
			applyCh <- applyMag
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// If last log index ≥ nextIndex for a follower: send
// AppendEntries RPC with log entries starting at nextIndex
// • If successful: update nextIndex and matchIndex for
// follower (§5.3)
// • If AppendEntries fails because of log inconsistency:
// decrement nextIndex and retry (§5.3)
func (rf *Raft) sendLogEntries(server int) {
	for {
		rf.mu.Lock()

		// 获取当前的任期、nextIndex、commitIndex
		term := rf.currentTerm
		nextIndex := rf.nextIndex[server]
		if rf.LastLogIndex() < nextIndex {
			// 不需要发日志了（可能已同步完）
			rf.mu.Unlock()
			return
		}

		// 构造 prevLogIndex / prevLogTerm
		prevLogIndex := rf.PrevLogIndex(server)
		prevLogTerm := rf.PrevLogTerm(server)

		// 拷贝日志 entries
		entries := make([]Log, len(rf.logs[nextIndex:]))
		copy(entries, rf.logs[nextIndex:])

		commitIndex := rf.commitIndex
		rf.mu.Unlock()

		// 构造 AppendEntriesArgs
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}

		reply := AppendEntriesReply{}
		DPrintf("server %d send AppendEntries to server %d, args: %+v\n", rf.me, server, args)
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

		if !ok {
			// 发送失败，网络原因等
			return
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			// 遇到更高 term，转换为 Follower
			rf.mu.Unlock()
			rf.convertToFollower(reply.Term)
			return
		}

		if reply.Success {
			// 日志复制成功，更新 nextIndex / matchIndex
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.mu.Unlock()
			return
		} else {
			// 日志冲突，退一步重试
			rf.nextIndex[server] = max(0, rf.nextIndex[server]-1)
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for N := rf.commitIndex + 1; N <= rf.LastLogIndex(); N++ {
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= N {
				count += 1
			}
		}
		if count >= (len(rf.peers)/2+1) && rf.logs[N].Term == rf.currentTerm {
			DPrintf("server %d update commitIndex to %d\n", rf.me, N)
			rf.commitIndex = N
		}
	}
}

func (rf *Raft) repeatSendHeartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.sendHeartBeat()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) repeatUpdateCommitIndex() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.updateCommitIndex()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) repeatSendLogEntries() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				rf.mu.Unlock()
				rf.sendLogEntries(server)
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.cond = sync.Cond{L: &rf.mu}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.hearedHeartBeat = false
	rf.state = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.logs = append(rf.logs, Log{Command: nil, Term: 0}) // dummy log entry

	rf.nextIndex = fillSlice(len(peers), rf.LastLogIndex()+1)
	rf.matchIndex = fillSlice(len(peers), 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.applyCommand(applyCh)

	go rf.repeatSendHeartBeat()

	go rf.repeatSendLogEntries()

	go rf.repeatUpdateCommitIndex()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
