package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) PrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) PrevLogTerm(server int) int {
	prevIndex := rf.PrevLogIndex(server)
	return rf.logs[rf.logs.RIndex(prevIndex)].Term
}

func (rf *Raft) electionTimerReset() {
	rf.electionTimer.Reset(time.Duration(50+rand.Int63()%300) * time.Millisecond)
}

func (rf *Raft) heartbeatTimerReset() {
	rf.heartbeatTimer.Reset(50 * time.Millisecond)
}

func (rf *Raft) writeCurrentTerm(term int) {
	if rf.currentTerm == term {
		return
	}
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) writeVotedFor(votedFor int) {
	if rf.votedFor == votedFor {
		return
	}
	rf.votedFor = votedFor
	rf.persist()
}

func (rf *Raft) appendLog(logs []Entries) {
	if len(logs) == 0 {
		return
	}
	rf.logs = append(rf.logs, logs...)
	rf.persist()
}
