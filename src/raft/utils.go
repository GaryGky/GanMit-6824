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

func (rf *Raft) isLogMissing(prevLogIndex, prevLogTerm int) (bool, int) {
	if prevLogIndex >= len(rf.LocalLog) {
		return true, len(rf.LocalLog) - 1
	}
	if rf.LocalLog[prevLogIndex].Term != int32(prevLogTerm) {
		return true, prevLogIndex - 1
	}
	return false, prevLogIndex
}

func (rf *Raft) isLogConflict(prevLogIndex int, remoteLogs []Log) (bool, int) {
	if prevLogIndex == len(rf.LocalLog)-1 {
		return false, prevLogIndex
	}
	localLogCopy := rf.LocalLog[prevLogIndex+1:]
	for i := 0; i < len(remoteLogs); i++ {
		if localLogCopy[i].Term != remoteLogs[i].Term {
			return true, i - 1
		}
	}
	return false, prevLogIndex
}
