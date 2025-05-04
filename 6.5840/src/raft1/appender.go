package raft

import (
	"time"
)

// If last log index ≥ nextIndex for a follower: send
// AppendEntries RPC with log entries starting at nextIndex
// • If successful: update nextIndex and matchIndex for
// follower (§5.3)
// • If AppendEntries fails because of log inconsistency:
// decrement nextIndex and retry (§5.3)
func (rf *Raft) appender(server int) {
	rf.appendCond[server].L.Lock()
	defer rf.appendCond[server].L.Unlock()
	for !rf.killed() {
		for !rf.canAppend(server) {
			rf.appendCond[server].Wait()
		}
		rf.appendOnce(server)
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) canAppend(server int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.nextIndex[server] <= rf.log.LastIndex()
}

func (rf *Raft) WakeAllAppender() {
	for server := range rf.peers {
		if server != rf.me {
			rf.appendCond[server].Signal()
		}
	}
}

func (rf *Raft) appendOnce(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.PrevLogIndex(server)
	if prevLogIndex < rf.log.FirstIndex() {
		rf.handleInstallSnapshotJob(server)
	} else {
		rf.handleAppendEntriesJob(server)
	}
}

func (rf *Raft) handleInstallSnapshotJob(server int) {
	args := rf.genInstallSnapshotArgs()
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()

	if rf.sendInstallSnapshot(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.DPrintf("send install snap shot to %d\n", server)
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			return
		}

		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
}

func (rf *Raft) handleAppendEntriesJob(server int) {
	args := rf.genAppendEntriesArgs(server)
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.DPrintf("send append entries %v to %d\n", args.Entries, server)
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			return
		}

		if reply.Success {
			rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			rf.updateCommitIndex()
		} else if reply.Confict {
			rf.quickUpdateNextIndex(server, reply.XTerm, reply.XIndex, reply.XLen)
			// rf.DPrintf("quick update next index %d to %d\n", server, rf.nextIndex[server])
			// rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
		}
	}
}

func (rf *Raft) quickUpdateNextIndex(server int, xterm int, xindex int, xlen int) {
	if xterm == -1 && xindex == -1 {
		rf.nextIndex[server] = xlen
		return
	}

	for i := min(rf.PrevLogIndex(server), rf.log.LastIndex()); i >= rf.log.FirstIndex(); i-- {
		if rf.log.Get(i).Term == xterm {
			rf.nextIndex[server] = i + 1
			return
		} else if rf.log.Get(i).Term < xterm {
			break
		}
	}

	rf.nextIndex[server] = max(min(xindex, rf.log.LastIndex()+1), rf.log.FirstIndex())
}

func (rf *Raft) broadcastHeartBeat() {
	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.appendOnce(server)
		}
	}
}
