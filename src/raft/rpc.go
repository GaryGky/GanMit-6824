package raft

import (
	"6.824/debug"
	"6.824/labrpc"
)

type Base struct {
	FromNodeID int
	ToNodeID   int
}

type BaseMessage interface {
	GetAllCaseByUserID() (int, int)
	PrintDebugInfo()
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// RequestVoteArgs Candidate start a election period
type RequestVoteArgs struct {
	LastLogIndex, LastLogTerm int
	Term                      int32
	Base                      Base
}

// RequestVoteReply Candidate receive reply from other candidate or follower
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
	Base        Base
}

type AppendEntryArgs struct {
	Term         int32
	PrevLogIndex int
	PrevLogTerm  int32
	LeaderCommit int32
	Entries      []Log
	Base         Base
}

type AppendEntryReply struct {
	Term    int32
	Success bool
	// when follower lost some logs, set this field to notify leader to reset next index
	LastMatchIndex int
	Base           Base
}

func (r *RequestVoteArgs) GetAllCaseByUserID() (int, int) {
	return r.Base.FromNodeID, r.Base.ToNodeID
}
func (r *RequestVoteArgs) PrintDebugInfo() {
	debug.Debug(debug.DClient, "S%d -> S%d, RequestVoteArgs:{Term: %d, LastLogIndex: %d, LastLogTerm: %d} \n",
		r.Base.FromNodeID, r.Base.ToNodeID, r.Term, r.LastLogIndex, r.LastLogTerm)
}

func (r *RequestVoteReply) GetAllCaseByUserID() (int, int) {
	return r.Base.FromNodeID, r.Base.ToNodeID
}
func (r *RequestVoteReply) PrintDebugInfo() {
	debug.Debug(debug.DClient, "S%d <- S%d, RequestVoteReply:{Term: %d, Granted: %v} \n",
		r.Base.ToNodeID, r.Base.FromNodeID, r.Term, r.VoteGranted)
}

func (r *AppendEntryArgs) GetAllCaseByUserID() (int, int) {
	return r.Base.FromNodeID, r.Base.ToNodeID
}
func (r *AppendEntryArgs) PrintDebugInfo() {
	debug.Debug(debug.DClient, "S%d -> S%d, AppendEntryArgs:{Term: %d, PrevLog: {Index: %d, Term: %d}, LeaderCommit: %d, Entries: %v} \n",
		r.Base.FromNodeID, r.Base.ToNodeID, r.Term, r.PrevLogIndex, r.PrevLogTerm, r.LeaderCommit, r.Entries)
}

func (r *AppendEntryReply) GetAllCaseByUserID() (int, int) {
	return r.Base.FromNodeID, r.Base.ToNodeID
}
func (r *AppendEntryReply) PrintDebugInfo() {
	debug.Debug(debug.DClient, "S%d <- S%d, AppendEntryReply: {Term: %d, Success: %v, LastMatchIndex: %d} \n",
		r.Base.ToNodeID, r.Base.FromNodeID, r.Term, r.Success, r.LastMatchIndex)
}

func rpcCall(endpoint *labrpc.ClientEnd, method string, args BaseMessage, reply BaseMessage) bool {
	args.PrintDebugInfo()
	ok := endpoint.Call(method, args, reply)
	if !ok {
		return false
	}
	reply.PrintDebugInfo()
	return ok
}
