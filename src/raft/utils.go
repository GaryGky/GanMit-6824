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

func isLogConflict(localLogs, remoteLogs []Log) bool {
	if len(localLogs) > len(remoteLogs) {
		return false
	}
	for i := 0; i < len(remoteLogs); i++ {
		if localLogs[i].Term != remoteLogs[i].Term {
			return false
		}
	}
	return true
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
