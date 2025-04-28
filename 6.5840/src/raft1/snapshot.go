package raft

import "time"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(rf.state, "server %v snapshot %v", rf.me, index)
	if rf.logStart > index {
		return
	}

	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)

	rf.leaderSendSnapshot()

	newlog := rf.log[index+1-rf.logStart:]
	rf.NewLog()
	rf.log = append(rf.log, newlog...)

	rf.logStart = index + 1
	rf.persist()
}

func (rf *Raft) leaderSendSnapshot() {
	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		args := rf.genInstallSnapshotArgs(rf.logStart)

		go func(server int) {
			reply := &InstallSnapshotReply{}
			for rf.sendInstallSnapshot(server, args, reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					return
				}
				rf.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
			}
		}(server)
	}
}
