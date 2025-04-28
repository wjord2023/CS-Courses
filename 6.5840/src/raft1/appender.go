package raft

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
	}
}

func (rf *Raft) canAppend(server int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.nextIndex[server] <= rf.LastLogIndex()
}

func (rf *Raft) WakeAllAppender() {
	if rf.state != Leader {
		panic("call WakeAllAppender when not leader")
	}

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

	args := rf.genAppendEntriesArgs(server)
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			rf.updateCommitIndex()
		} else {
			if reply.Confict {
				// rf.quickUpdateNextIndex(server, reply.XTerm, reply.XIndex, reply.XLen)
				rf.quickUpdateNextIndex(server, reply.XTerm, reply.XIndex, reply.XLen)
				// rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) quickUpdateNextIndex(server int, xterm int, xindex int, xlen int) {
	defer DPrintf(rf.state, "server %v quick update next index %v", rf.me, rf.nextIndex)
	if xterm == -1 && xindex == -1 {
		rf.nextIndex[server] = xlen
	}

	for i := min(rf.PrevLogIndex(server), rf.LastLogIndex()); i >= rf.logStart; i-- {
		if rf.log[i-rf.logStart].Term == xterm {
			rf.nextIndex[server] = i + 1
			return
		} else if rf.log[i-rf.logStart].Term < xterm {
			break
		}
	}

	rf.nextIndex[server] = max(min(xindex, rf.LastLogIndex()+1), rf.logStart)
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
