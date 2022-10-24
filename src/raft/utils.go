package raft

import (
	"math/rand"
	rDebug "runtime/debug"
	"time"

	"6.824/debug"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
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

func safeGo(fun func()) {
	defer RecoverAndLog()
	fun()
}

func randomTime() time.Duration {
	return time.Duration(rand.Intn(10)) * time.Millisecond
}

func canVoteForCandidate(voteFor int32, reqTerm, currentTerm int32, fromNodeID int) bool {
	return reqTerm > currentTerm || voteFor == -1 || fromNodeID == int(voteFor)
}
