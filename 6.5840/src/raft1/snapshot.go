package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = rf.logs[rf.logs.RIndex(index+1):]
	rf.persist()
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)

	if rf.state == Leader {
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.nextIndex[server] < index {
					args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: index, LastIncludedTerm: rf.logs[rf.logs.RIndex(index)].Term}
					reply := InstallSnapshotReply{}

					ok := false
					for ok {
						ok = rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
					}

					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
					}
				}
			}(server)
		}
	}
}
