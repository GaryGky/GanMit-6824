package raft

import (
	"math/rand"
	rDebug "runtime/debug"
	"sync"
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

func randomTime(duration time.Duration) time.Duration {
	return time.Duration(rand.Intn(10))*time.Millisecond + duration
}

func clearSyncMap(p *sync.Map) {
	p.Range(func(key interface{}, value interface{}) bool {
		p.Delete(key)
		return true
	})
}

func isRaftAbleToGrantVote(arg *RequestVoteArgs, rf *Raft) bool {
	// compare local log with log in request
	isArgLogLatest := func(arg *RequestVoteArgs, rf *Raft) bool {
		myLastIndex, myLastTerm := rf.getLastLogIndexAndTerm()
		// same term: the raft has longer log is the latest
		if myLastTerm == arg.LastLogTerm {
			return myLastIndex <= arg.LastLogIndex
		}
		// different term: the raft has higher term is the latest
		return myLastTerm <= arg.LastLogTerm
	}

	// case1: The Request Vote is sent by outdated candidate or the message is delayed in the network
	if arg.Term < (rf.CurrentTerm.Load()) {
		return false
	} else if arg.Term == (rf.CurrentTerm.Load()) {
		// self voting
		if int(rf.voteFor.Load()) == arg.Base.FromNodeID {
			return true
		}
		// case2: check if the node voted for someone or the coming candidate
		if rf.voteFor.Load() != -1 && int(rf.voteFor.Load()) != arg.Base.FromNodeID {
			return false
		}
		if rf.voteFor.Load() == -1 {
			return isArgLogLatest(arg, rf)
		}
	} else {
		// case3: higher term candidate want to compete for leadership
		defer rf.becomeFollower(-1)

		rf.voteFor.Store(-1)
		rf.CurrentTerm.Store(arg.Term)
		return isArgLogLatest(arg, rf)
	}
	return true
}

func printDisconnected(server ...int) {
	debug.Debug(debug.DTest, "%v is disconnected from network \n", server)
}

func printConnected(server ...int) {
	debug.Debug(debug.DTest, "%v is connected from network \n", server)
}
