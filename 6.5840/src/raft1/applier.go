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
