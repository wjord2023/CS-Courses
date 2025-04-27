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
	return rf.log[prevIndex-rf.logStart].Term
}

func (rf *Raft) electionTimerReset() {
	rf.electionTimer.Reset(time.Duration(50+rand.Int63()%300) * time.Millisecond)
}

func (rf *Raft) heartbeatTimerReset() {
	rf.heartbeatTimer.Reset(50 * time.Millisecond)
}
