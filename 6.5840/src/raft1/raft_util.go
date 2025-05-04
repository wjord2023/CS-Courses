package raft

import (
	"fmt"
	"math/rand"
	"time"
)

const Debug = false

func (rf *Raft) PrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) PrevLogTerm(server int) int {
	prevIndex := rf.PrevLogIndex(server)
	if prevIndex < 0 {
		panic(fmt.Sprintf("server %d prevIndex %d < 0", server, prevIndex))
	}
	return rf.log.Get(prevIndex).Term
}

func (rf *Raft) electionTimerReset() {
	rf.electionTimer.Reset(time.Duration(750+rand.Int63()%750) * time.Millisecond)
}

func (rf *Raft) heartbeatTimerReset() {
	rf.heartbeatTimer.Reset(50 * time.Millisecond)
}

func (rf *Raft) DPrintf(format string, a ...any) {
	if Debug {
		format = fmt.Sprintf("server %d ", rf.me) + format

		if rf.state == Leader {
			format = "\033[31m[Leader]\033[0m  " + format
		}
		if rf.state == Follower {
			format = "\033[32m[Follower]\033[0m  " + format
		}
		if rf.state == Candidate {
			format = "\033[33m[Candidate]\033[0m  " + format
		}

		fmt.Printf(format, a...)
	}
}

func (rf *Raft) Shrink(index int) {
	rf.DPrintf("log before shrink %v\n", rf.log)
	newL := make([]Entries, 0)
	newL = append(newL, rf.log[index-rf.log.FirstIndex():]...)
	rf.log = newL
	rf.log[0].Command = nil
	rf.DPrintf("log shrink to %v\n", rf.log)
}

func (rf *Raft) RenewLog(firstIndex int, firstTerm int) {
	newL := make(Log, 0)
	newL = append(newL, Entries{Command: nil, Term: firstTerm, Index: firstIndex})
	rf.log = newL
}
