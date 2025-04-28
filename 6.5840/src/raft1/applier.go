package raft

import "6.5840/raftapi"

func (rf *Raft) applier() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()

	for !rf.killed() {
		for !(rf.commitIndex > rf.lastApplied) {
			rf.applyCond.Wait()
		}
		rf.lastApplied += 1
		command := rf.log[rf.lastApplied-rf.logStart].Command
		if command == nil {
			continue
		}
		index := rf.log[rf.lastApplied-rf.logStart].Index
		applyMag := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
		rf.applyCh <- applyMag
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (rf *Raft) updateCommitIndex() {
	for N := rf.LastLogIndex(); rf.commitIndex < N; N-- {
		count := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				count += 1
			}
		}
		if count >= (len(rf.peers)/2+1) && rf.log[N-rf.logStart].Term == rf.currentTerm {
			rf.commitIndex = N
			rf.applyCond.Signal()
			DPrintf(rf.state, "server %v update commit index to %v", rf.me, rf.commitIndex)
			rf.broadcastHeartBeat()
			return
		}
	}
}
