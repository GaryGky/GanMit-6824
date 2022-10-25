package raft

import (
	"math/rand"
	rDebug "runtime/debug"
	"time"

	"6.824/debug"
)

func minInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func RecoverAndLog() {
	if err := recover(); err != nil {
		stack := string(rDebug.Stack())
		debug.Debug(debug.DError, "%v \n", stack)
	}
}

func isLogConflict(afterLog, replicateLogs []Log) bool {
	if len(afterLog) > len(replicateLogs) {
		return false
	}
	for i := 0; i < len(replicateLogs); i++ {
		if afterLog[i].Term != replicateLogs[i].Term {
			return false
		}
	}
	return true
}

func randomTime() time.Duration {
	return time.Duration(rand.Intn(10)) * time.Millisecond
}

func canVoteForCandidate(voteFor int32, reqTerm, currentTerm int32, fromNodeID int) bool {
	return reqTerm > currentTerm || voteFor == -1 || fromNodeID == int(voteFor)
}
