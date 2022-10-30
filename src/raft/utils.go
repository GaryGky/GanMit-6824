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

func randomTime() time.Duration {
	return time.Duration(rand.Intn(10)) * time.Millisecond
}

func clearSyncMap(p *sync.Map) {
	p.Range(func(key interface{}, value interface{}) bool {
		p.Delete(key)
		return true
	})
}

func isRaftAbleToGrantVote(arg *RequestVoteArgs, rf *Raft) bool {
	// case1: The Request Vote is sent by outdated candidate or the message is delayed in the network
	if arg.Term < (rf.CurrentTerm.Load()) {
		return false
	} else if arg.Term == (rf.CurrentTerm.Load()) {
		// case2: check if the node voted for someone or the coming candidate
		if rf.voteFor.Load() != -1 && int(rf.voteFor.Load()) != arg.Base.FromNodeID {
			return false
		}
		if rf.voteFor.Load() == -1 || int(rf.voteFor.Load()) == arg.Base.FromNodeID {
			if ok := isArgLogLatest(arg, rf); ok {
				rf.voteFor.Store(int32(arg.Base.FromNodeID))
				return int(rf.voteFor.Load()) == arg.Base.FromNodeID
			} else {
				return false
			}
		}
	} else {
		// case3: higher term candidate want to compete for leadership
		defer rf.becomeFollower()

		rf.voteFor.Store(-1)
		rf.CurrentTerm.Store(arg.Term)
		return isArgLogLatest(arg, rf)
	}
	return true
}

// compare local log with log in request
func isArgLogLatest(arg *RequestVoteArgs, rf *Raft) bool {
	myLastIndex, myLastTerm := rf.getLastLogIndexAndTerm()
	// case3.1: the candidate's log is not up-to-date
	if myLastTerm > arg.LastLogTerm {
		return false
	}
	if myLastTerm == arg.LastLogTerm {
		// case3.2: the candidate log is not up-to-date
		if myLastIndex > arg.LastLogIndex {
			return false
		} else {
			// case3.3: the candidate log is up-to-date
			return true
		}
	}
	// case4: the candidate log is up-to-date
	if myLastTerm < arg.LastLogTerm {
		return true
	}
	return true
}

func getAppendingLogs(nextIndex int, rf *Raft) []Log {
	updateUncommittedLogsTerm := func(rf *Raft) {
		for i := int(rf.CommitIndex.Load()); i < len(rf.LocalLog); i++ {
			rf.LocalLog[i].Term = rf.CurrentTerm.Load()
		}
	}

	if nextIndex >= len(rf.LocalLog) {
		return []Log{}
	}
	updateUncommittedLogsTerm(rf)
	return rf.LocalLog[nextIndex:]
}

func printDisconnected(server ...int) {
	debug.Debug(debug.DTest, "%v is disconnected from network \n", server)
}

func printConnected(server ...int) {
	debug.Debug(debug.DTest, "%v is connected from network \n", server)
}
