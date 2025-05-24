package raft

import (
	"6.5840/raftapi"
)

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.commitIndex > rf.lastApplied) {
			rf.applyCond.Wait()
		}
		if rf.lastApplied < rf.log.FirstIndex() {
			rf.applySnapshotCommand()
		} else {
			rf.applyLogCommand()
		}
	}
}

func (rf *Raft) applyLogCommand() {
	rf.DPrintf("apply log to index %d\n", rf.lastApplied)
	rf.lastApplied += 1
	command := rf.log.Get(rf.lastApplied).Command
	if command == nil {
		return
	}
	index := rf.log.Get(rf.lastApplied).Index
	applyMag := raftapi.ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: index,
	}
	rf.mu.Unlock()
	select {
	case rf.applyCh <- applyMag:
	case <-rf.stop:
		close(rf.applyCh)
	}
}

func (rf *Raft) applySnapshotCommand() {
	rf.DPrintf("apply snapshot to index %d\n", rf.log.FirstIndex())
	rf.lastApplied = rf.log.FirstIndex()
	applyMsg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  rf.log.FirstTerm(),
		SnapshotIndex: rf.log.FirstIndex(),
	}
	rf.mu.Unlock()

	select {
	case rf.applyCh <- applyMsg:
	case <-rf.stop:
		close(rf.applyCh)
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (rf *Raft) updateCommitIndex() {
	for N := rf.log.LastIndex(); max(rf.commitIndex, rf.log.FirstIndex()) < N; N-- {
		count := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				count += 1
			}
		}
		if count >= (len(rf.peers)/2+1) && rf.log.Get(N).Term == rf.currentTerm {
			rf.commitIndex = N
			rf.DPrintf("update commit index to %d\n", rf.commitIndex)
			rf.applyCond.Signal()
			rf.broadcastHeartBeat()
			return
		}
	}
}
