package raft

import "log"

// Debugging
const Debug = false

func DPrintf(state ServerState, format string, a ...interface{}) {
	if Debug {
		if state == Leader {
			format = "\033[31m[Leader]\033[0m  " + format
		}
		if state == Follower {
			format = "\033[32m[Follower]\033[0m  " + format
		}
		if state == Candidate {
			format = "\033[33m[Candidate]\033[0m  " + format
		}
		log.Printf(format, a...)
	}
}

func fillSlice[T any](n int, value T) []T {
	s := make([]T, n)
	for i := range s {
		s[i] = value
	}
	return s
}
