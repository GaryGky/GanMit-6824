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
	// Your data here (2A, 2B).
	Term int
	Base Base
}

// RequestVoteReply Candidate receive reply from other candidate or follower
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
	Base        Base
}

type AppendEntryArgs struct {
	Term int
	Base Base
}

type AppendEntryReply struct {
	Term    int
	Success bool
	Base    Base
}

func (r *RequestVoteArgs) GetAllCaseByUserID() (int, int) {
	return r.Base.FromNodeID, r.Base.ToNodeID
}

func (r *RequestVoteReply) GetAllCaseByUserID() (int, int) {
	return r.Base.FromNodeID, r.Base.ToNodeID
}

func (r *AppendEntryArgs) GetAllCaseByUserID() (int, int) {
	return r.Base.FromNodeID, r.Base.ToNodeID
}

func (r *AppendEntryReply) GetAllCaseByUserID() (int, int) {
	return r.Base.FromNodeID, r.Base.ToNodeID
}

func rpcCall(endpoint *labrpc.ClientEnd, method string, args BaseMessage, reply BaseMessage) bool {
	from, to := args.GetAllCaseByUserID()
	debug.Debug(debug.DClient, "S%d -> S%d, %s request: %v \n", from, to, method, args)
	ok := endpoint.Call(method, args, reply)
	debug.Debug(debug.DClient, "S%d <- S%d, %s response: %v \n", from, to, method, reply)
	return ok
}
