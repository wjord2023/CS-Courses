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
	return rf.state == Leader && rf.nextIndex[server] <= rf.logs.LastIndex()
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
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}

	args := rf.genAppendEntriesArgs(server)
	reply := &AppendEntriesReply{}

	rf.mu.RUnlock()
	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			if args.Entries != nil {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1

				DPrintf(rf.state, "server %v send append entries to %v success", rf.me, server)
				rf.updateCommitIndex()
			}
		} else {
			rf.quickUpdateNextIndex(server, reply)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) quickUpdateNextIndex(server int, reply *AppendEntriesReply) {
	if reply.XTerm == -1 && reply.XIndex == -1 {
		rf.nextIndex[server] = reply.XLen
		return
	}

	leaderHasXterm := false
	for i := rf.PrevLogIndex(server); i >= 0; i-- {
		if rf.logs[rf.logs.RIndex(i)].Term == reply.XTerm {
			rf.nextIndex[server] = i + 1
			leaderHasXterm = true
			break
		}
	}

	if !leaderHasXterm {
		rf.nextIndex[server] = reply.XIndex
	}
}

func (rf *Raft) broadcastHeartBeat() {
	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			// 	go func(server int) {
			// 		rf.mu.RLock()
			// 		args := AppendEntriesArgs{
			// 			rf.currentTerm,
			// 			rf.PrevLogIndex(server),
			// 			rf.PrevLogTerm(server),
			// 			nil,
			// 			rf.commitIndex,
			// 		}
			// 		reply := AppendEntriesReply{}
			// 		rf.mu.RUnlock()
			// 		rf.sendAppendEntries(server, &args, &reply)
			// 	}(server)
			// }
			go rf.appendOnce(server)
		}
	}
}
