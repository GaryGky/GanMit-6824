package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/debug"
	//	"6.824/labgob"
	"6.824/labrpc"
)

var (
	// ElectionTimeout Within the range if there's no heartbeat from leader, Raft will start an election
	ElectionTimeout = time.Millisecond * 500
	// HeartBeatTimeout within the range leader should send a heartbeat to all server
	HeartBeatTimeout = time.Millisecond * 250
	// ApplyTimeout within the range, every node should check local logs from (lastApplied, commitIndex]
	ApplyTimeout = time.Millisecond * 500
)

type Log struct {
	Term    int32
	Command interface{}
}

// Raft object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // PhaseLock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // (READ ONLY) RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      atomic.Int32        // set by Kill()

	// All State
	LocalLog         []Log
	CurrentTerm      atomic.Int32
	LeaderID         atomic.Int32
	CommitIndex      atomic.Int32
	LastAppliedIndex atomic.Int32
	applyChan        chan ApplyMsg

	// Follower States
	IsLeaderAlive atomic.Bool // Follower to check whether it should start a new election
	voteFor       atomic.Int32
	// Candidate States
	VotesFromPeers atomic.Int32

	// Leader States
	NextIndex  sync.Map
	MatchIndex sync.Map
	LogAck     sync.Map
}

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.CurrentTerm.Load()), int(rf.LeaderID.Load()) == rf.me
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// Start the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	if !rf.isLeader() {
		return index, term, false
	}
	rf.LocalLog = append(rf.LocalLog, Log{
		Term:    rf.CurrentTerm.Load(),
		Command: command,
	})
	// Leader should start synchronize log here

	for i := range rf.peers {
		go func(server int) {
			RecoverAndLog()
			reply := AppendEntryReply{}
			nextIndex, ok := rf.NextIndex.Load(server)
			if !ok {
				debug.Debug(debug.DError, "S%d GetNextIndex in Log failed, to S%d \n", rf.me, server)
				return
			}

			li, lt := rf.getPrevLogIndexAndTerm(nextIndex.(int))
			ok = rf.sendAppendEntry(server, &AppendEntryArgs{
				Term:         int(rf.CurrentTerm.Load()),
				PrevLogIndex: li,
				PrevLogTerm:  lt,
				LeaderCommit: rf.CommitIndex.Load(),
				Entries:      rf.LocalLog,
				Base: Base{
					FromNodeID: rf.me,
					ToNodeID:   server,
				},
			}, &reply)
			if !ok {
				debug.Debug(debug.DError, "S%d AppendEntry failed, to S%d \n", rf.me, server)
				return
			}
			rf.processAppendEntryReply(reply, li, server)
		}(i)
	}
	return index, term, rf.isLeader()
}

func (rf *Raft) processAppendEntryReply(reply AppendEntryReply, li int, server int) bool {
	if reply.Term > int(rf.CurrentTerm.Load()) {
		rf.becomeFollower()
		return true
	}
	if reply.Success {
		v, ok := rf.LogAck.Load(li + 1)
		if !ok {
			rf.LogAck.Store(li+1, 1)
		} else {
			rf.LogAck.Store(li+1, v.(int)+1)
		}

		matchIndex, ok := rf.MatchIndex.Load(server)
		if !ok {
			return false
		}
		rf.MatchIndex.Store(server, matchIndex.(int)+1)
		nextIndex, ok := rf.NextIndex.Load(server)
		if !ok {
			return false
		}
		rf.NextIndex.Store(server, nextIndex.(int)+1)
		if v.(int)+1 == (len(rf.peers)+1)/2 {
			// TODO: leader can commit here
			rf.CommitIndex.Add(1)
		}
	}
	return true
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	rf.dead.Store(1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := rf.dead.Load()
	return z == 1
}

// Make
// peers: the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order.
// persister: is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any.
// applyCh: is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}

	// Your initialization code here (2A, 2B, 2C).
	rf.VotesFromPeers = atomic.Int32{}
	rf.LeaderID = atomic.Int32{}
	rf.CurrentTerm = atomic.Int32{}
	rf.voteFor = atomic.Int32{}
	rf.voteFor.Store(-1)
	rf.IsLeaderAlive.Store(false)
	rf.LocalLog = make([]Log, 0)
	rf.CommitIndex = atomic.Int32{}
	rf.CommitIndex.Store(-1)
	rf.LastAppliedIndex = atomic.Int32{}
	rf.LastAppliedIndex.Store(-1)
	rf.applyChan = make(chan ApplyMsg)
	rf.NextIndex = sync.Map{}
	rf.MatchIndex = sync.Map{}
	rf.LogAck = sync.Map{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.committedLogTimer()

	return rf
}

// -------- Timer ---------------

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		if !rf.IsLeaderAlive.Load() && rf.voteFor.Load() == int32(-1) {
			rf.startElection()
		}
		rf.IsLeaderAlive.Store(false)
		rf.voteFor.Store(-1)
		time.Sleep(ElectionTimeout)
		time.Sleep(randomTime())
	}
}

func (rf *Raft) heartBeat() {
	for rf.isLeader() {
		for i := range rf.peers {
			req := &AppendEntryArgs{
				Term: int(rf.CurrentTerm.Load()),
				Base: Base{
					FromNodeID: rf.me,
					ToNodeID:   i,
				},
			}
			go func(server int) {
				reply := &AppendEntryReply{}
				ok := rf.sendAppendEntry(server, req, reply)
				if !ok {
					debug.Debug(debug.DError, "S%d -> S%d, heartbeat failed \n", rf.me, server)
				}
				if !reply.Success {
					rf.becomeFollower()
				}
			}(i)
		}
		time.Sleep(HeartBeatTimeout)
	}
}

// Timer: put committed log into applyChannel
func (rf *Raft) committedLogTimer() {
	for rf.dead.Load() != 1 {
		rf.mu.Lock()
		applyingLogs := rf.LocalLog[rf.LastAppliedIndex.Load():rf.CommitIndex.Load()]
		rf.mu.Unlock()
		lastAppliedIndex := rf.LastAppliedIndex.Load()
		for i, applyingLog := range applyingLogs {
			rf.applyChan <- ApplyMsg{
				Command:      applyingLog.Command,
				CommandIndex: int(lastAppliedIndex) + i,
			}
		}
		time.Sleep(ApplyTimeout)
	}
}

// -------- RPC Invoke -----------

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	endPoint := rf.peers[server]
	ok := rpcCall(endPoint, "Raft.RequestVote", args, reply)
	if !ok {
		return false
	}

	if reply.VoteGranted {
		rf.VotesFromPeers.Add(1)
		if rf.VotesFromPeers.Load() == int32((len(rf.peers)+1)/2) {
			rf.becomeLeader()
		}
	}
	return ok
}

// sendAppendEntry in heartbeat and log replication
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	return rpcCall(rf.peers[server], "Raft.AppendEntry", args, reply)
}

// -------- RPC Handler ----------

// RequestVote RPC handler.
func (rf *Raft) RequestVote(arg *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Base.ToNodeID = arg.Base.FromNodeID
	reply.Base.FromNodeID = rf.me
	reply.Term = maxInt32(rf.CurrentTerm.Load(), arg.Term)

	// case1: The Request Vote is sent by outdated candidate or the message is delayed in the network
	if arg.Term < (rf.CurrentTerm.Load()) {
		reply.VoteGranted = false
		return
	} else if arg.Term == (rf.CurrentTerm.Load()) {
		// case2: check if the node voted for someone or the coming candidate
		if rf.voteFor.Load() != -1 && int(rf.voteFor.Load()) != arg.Base.FromNodeID {
			reply.VoteGranted = false
			return
		}
		if rf.voteFor.Load() == -1 || int(rf.voteFor.Load()) == arg.Base.FromNodeID {
			reply.VoteGranted = true
			rf.voteFor.Store(int32(arg.Base.FromNodeID))
			return
		}
	} else {
		// case3: higher term candidate want to compete for leadership
		rf.voteFor.Store(-1)
		rf.CurrentTerm.Store(arg.Term)
		reply.Term = rf.CurrentTerm.Load()
		defer rf.becomeFollower()
		myLastIndex, myLastTerm := rf.getLastLogIndexAndTerm()
		// case3.1: the candidate's log is not up-to-date
		if myLastTerm > arg.LastLogTerm {
			reply.VoteGranted = false
			return
		}
		if myLastTerm == arg.LastLogTerm {
			// case3.2: the candidate log is not up-to-date
			if myLastIndex > arg.LastLogIndex {
				reply.VoteGranted = false
				return
			} else {
				// case3.3: the candidate log is up-to-date
				rf.voteFor.Store(int32(arg.Base.FromNodeID))
				reply.VoteGranted = true
				return
			}
		}
		// case4: the candidate log is up-to-date
		if myLastTerm < arg.LastLogTerm {
			rf.voteFor.Store(int32(arg.Base.FromNodeID))
			reply.VoteGranted = true
			return
		}
	}
}

func (rf *Raft) AppendEntry(arg *AppendEntryArgs, reply *AppendEntryReply) {
	from, _ := arg.GetAllCaseByUserID()
	reply.Term = int(rf.CurrentTerm.Load())

	if arg.Term < int(rf.CurrentTerm.Load()) {
		reply.Success = false
		return
	}
	// become follower
	rf.becomeFollower()
	rf.CurrentTerm.Store(int32(arg.Term))
	rf.LeaderID.Swap(int32(from))
	rf.voteFor.Store(int32(from))
	rf.IsLeaderAlive.Store(true)

	rf.mu.Lock()
	// no log in prevLogIndex or prevLogIndex has different term
	if len(rf.LocalLog) <= arg.PrevLogIndex || int(rf.LocalLog[arg.PrevLogIndex].Term) != arg.PrevLogTerm {
		reply.Success = false
		reply.Term = int(rf.CurrentTerm.Load())

		return
	}

	// store replicated log into local logs
	if isLogConflict(rf.LocalLog[arg.PrevLogIndex:], arg.Entries) {
		rf.LocalLog = append(rf.LocalLog[:arg.PrevLogIndex+1], arg.Entries...)
	}
	if arg.LeaderCommit > rf.CommitIndex.Load() {
		rf.CommitIndex.Store(minInt32(arg.LeaderCommit, int32(len(rf.LocalLog)-1)))
	}
	reply.Success = true
}

// -------- utils ---------

func (rf *Raft) becomeLeader() {
	debug.Debug(debug.DLeader, "S%d becomes Leader in Term %d \n", rf.me, rf.CurrentTerm.Load())
	rf.LeaderID.Store(int32(rf.me))
	rf.initNextAndMatch()
	go rf.heartBeat()
}

func (rf *Raft) initNextAndMatch() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := range rf.peers {
		rf.NextIndex.Store(server, len(rf.LocalLog))
		rf.MatchIndex.Store(server, 0)
	}
}

func (rf *Raft) becomeCandidate() {
	rf.voteFor.Store(-1)
	rf.VotesFromPeers.Swap(0)
	rf.CurrentTerm.Add(1)
	debug.Debug(debug.DLeader, "S%d becomes Candidate in Term %d \n", rf.me, rf.CurrentTerm.Load())
}

func (rf *Raft) becomeFollower() {
	rf.voteFor.Store(-1)
	rf.VotesFromPeers.Store(0)
	rf.LeaderID.Store(-1)
	rf.IsLeaderAlive.Store(false)
	debug.Debug(debug.DLeader, "S%d becomes Follower in Term %d \n", rf.me, rf.CurrentTerm.Load())
}

func (rf *Raft) isLeader() bool {
	return int(rf.LeaderID.Load()) == rf.me
}

func (rf *Raft) getPrevLogIndexAndTerm(nextIndex int) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if nextIndex < 1 {
		return 0, 0
	}
	return nextIndex - 1, int(rf.LocalLog[nextIndex-1].Term)
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	l := len(rf.LocalLog)
	if l == 0 {
		return 0, 0
	}
	return l - 1, int(rf.LocalLog[l-1].Term)
}

func (rf *Raft) startElection() {
	rf.becomeCandidate()

	// send RequestVoteRPC to all peers
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	for i := range rf.peers {
		req := &RequestVoteArgs{
			Term:         rf.CurrentTerm.Load(),
			LastLogTerm:  lastLogTerm,
			LastLogIndex: lastLogIndex,
			Base: Base{
				FromNodeID: rf.me,
				ToNodeID:   i,
			},
		}
		go func(server int) {
			defer RecoverAndLog()
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, req, reply)
			if !ok {
				debug.Debug(debug.DError, "S%d startElection failed, to S%d \n", rf.me, server)
			}
		}(i)
	}
}
