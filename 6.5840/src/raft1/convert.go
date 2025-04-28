package raft

func (rf *Raft) convertToFollower(term int) {
	DPrintf(rf.state, "[term %d] server %v convert to follower", rf.currentTerm, rf.me)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()

	rf.state = Follower
}

func (rf *Raft) convertToLeader() {
	DPrintf(rf.state, "[term %d] server %v convert to leader", rf.currentTerm, rf.me)

	rf.state = Leader
	rf.nextIndex = fillSlice(len(rf.peers), rf.LastLogIndex()+1)
	rf.matchIndex = fillSlice(len(rf.peers), 0)
	rf.AppendLog(nil)
	rf.WakeAllAppender()
}

func (rf *Raft) convertToCondidate() {
	DPrintf(rf.state, "[term %d] server %v convert to candidate", rf.currentTerm, rf.me)
	rf.state = Candidate
	rf.currentTerm += 1
	rf.persist()

	// start election
	go rf.startElection()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = rf.me
	rf.persist()

	numVotes := 1

	args := rf.genRequestVoteArgs()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				if rf.state != Candidate || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					numVotes += 1
					if numVotes >= (len(rf.peers)/2 + 1) {
						rf.convertToLeader()
						numVotes = -99 // prevent double conversion
					}
				} else if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
				}

				rf.mu.Unlock()
			}
		}(server)
	}
}
