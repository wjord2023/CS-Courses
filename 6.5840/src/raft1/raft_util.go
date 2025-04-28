package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func (rf *Raft) PrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) PrevLogTerm(server int) int {
	prevIndex := rf.PrevLogIndex(server)
	if prevIndex < 0 {
		panic(fmt.Sprintf("server %d prevIndex %d < 0", server, prevIndex))
	}
	return rf.log[prevIndex-rf.logStart].Term
}

func (rf *Raft) electionTimerReset() {
	rf.electionTimer.Reset(time.Duration(750+rand.Int63()%750) * time.Millisecond)
}

func (rf *Raft) heartbeatTimerReset() {
	rf.heartbeatTimer.Reset(50 * time.Millisecond)
}

func (rf *Raft) LastLogIndex() int {
	return rf.logStart + len(rf.log) - 1
}

func (rf *Raft) LastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) NewLog() {
	rf.log = []Entries{{nil, -1, 0}}
	rf.logStart = 0
}

func (rf *Raft) AppendLog(command any) int {
	var index int
	if command == nil {
		index = rf.LastCommandIndex() // no-op command don't update command index
	} else {
		index = rf.LastCommandIndex() + 1
	}
	rf.log = append(rf.log, Entries{command, rf.currentTerm, index})
	rf.persist()
	return index
}

func (rf *Raft) LastCommandIndex() int {
	return rf.log[len(rf.log)-1].Index
}
