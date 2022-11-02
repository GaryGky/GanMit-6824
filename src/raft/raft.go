package raft

import (
	"bytes"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/debug"
	"6.824/labgob"

	//	"6.824/labgob"
	"6.824/labrpc"
)

var (
	// ElectionTimeout Within the range if there's no heartbeat from leader, Raft will start an election
	ElectionTimeout = time.Millisecond * 900
	// HeartBeatTimeout within the range leader should send a heartbeat to all server
	HeartBeatTimeout = time.Millisecond * 300
	// ApplyTimeout within the range, every node should check local logs from (lastApplied, commitIndex]
	ApplyTimeout = time.Millisecond * 500
)

type State int

const (
	Leader    State = 1
	Candidate State = 2
	Follower  State = 3
)

type Log struct {
	Term    int32
	Command interface{}
}

type AlreadyApplyLog struct {
	index int
	term  int32
}

// Raft object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // PhaseLock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // (READ ONLY) RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              atomic.Int32        // set by Kill()
	handlerTimerMutex sync.Mutex

	// All State
	LocalLog         []Log
	CurrentTerm      atomic.Int32
	LeaderID         atomic.Int32
	CommitIndex      atomic.Int32
	LastAppliedIndex atomic.Int32
	applyChan        chan ApplyMsg
	state            atomic.Value

	// Follower States
	voteFor atomic.Int32
	// Candidate States
	VotesFromPeers atomic.Int32

	// Leader States
	NextIndex              sync.Map
	MatchIndex             sync.Map
	receivedClientRequests sync.Map
	failedRPCCounter       sync.Map
}

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.CurrentTerm.Load()), int(rf.LeaderID.Load()) == rf.me
}

// save Raft's persistent state to stable storage, where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm.Load())
	e.Encode(rf.voteFor.Load())

	rf.mu.Lock()
	e.Encode(rf.LocalLog)
	rf.mu.Unlock()

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// bootstrap without any state
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int32
	var logs []Log
	if err := d.Decode(&currentTerm); err != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		debug.Debug(debug.DError, "readPersist Decode error: %v \n")
	} else {
		rf.mu.Lock()
		rf.LocalLog = logs
		rf.mu.Unlock()

		rf.CurrentTerm.Store(currentTerm)
		rf.voteFor.Store(votedFor)
	}
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

// Start the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index, term, isLeader := rf.PreProcessLog(command)
	if !isLeader {
		return index, int(term), false
	}
	rf.SyncLogs(term)
	return index, int(term), rf.isLeader()
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
	return rf.dead.Load() == 1
}

// Make persister: is a place for this server to save its persistent state, and also initially holds the most recent saved state, if any.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.handlerTimerMutex = sync.Mutex{}
	rf.applyChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.VotesFromPeers = atomic.Int32{}
	rf.LeaderID = atomic.Int32{}
	rf.CurrentTerm = atomic.Int32{}
	rf.voteFor = atomic.Int32{}
	rf.voteFor.Store(-1)

	rf.LocalLog = make([]Log, 0)
	rf.CommitIndex = atomic.Int32{}
	rf.LastAppliedIndex = atomic.Int32{}
	rf.CommitIndex.Store(0)
	rf.LastAppliedIndex.Store(0)
	// append a nop log into local log
	rf.mu.Lock()
	rf.LocalLog = append(rf.LocalLog, Log{Term: 0})
	rf.mu.Unlock()

	rf.NextIndex = sync.Map{}
	rf.MatchIndex = sync.Map{}
	rf.state = atomic.Value{}
	rf.state.Store(Follower)
	rf.receivedClientRequests = sync.Map{}
	rf.failedRPCCounter = sync.Map{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ElectionTimer goroutine to start elections
	rf.becomeFollower(-1)
	go rf.ElectionTimer()
	go rf.committedLogTimer()

	return rf
}

// -------- Timer ---------------

// The ElectionTimer go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ElectionTimer() {
	for rf.killed() == false {
		rf.handlerTimerMutex.Lock()
		if rf.LeaderID.Load() == -1 && rf.voteFor.Load() == int32(-1) {
			rf.startElection()
		}
		rf.handlerTimerMutex.Unlock()
		if !rf.isLeader() {
			rf.LeaderID.Store(-1)
			rf.voteFor.Store(-1)
			time.Sleep(ElectionTimeout)
			time.Sleep(randomTime())
		}
	}
}

func (rf *Raft) heartBeat() {
	for !rf.killed() && rf.isLeader() {
		rf.SyncLogs(rf.CurrentTerm.Load())
		time.Sleep(HeartBeatTimeout)
	}
}

// Timer: put committed log into applyChannel
func (rf *Raft) committedLogTimer() {
	for !rf.killed() {
		// flushLocalLog put the log into channel so that it should be lock-free
		// copy the local log and pass it to flushLocalLog
		rf.mu.Lock()
		log := make([]Log, len(rf.LocalLog))
		copy(log, rf.LocalLog)
		committedIndex := rf.CommitIndex.Load()
		rf.mu.Unlock()

		rf.flushLocalLog(log, int(committedIndex))
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
	return rpcCall(rf.peers[server], "Raft.RequestVote", args, reply)
}

// sendAppendEntry in heartbeat and log replication
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	return rpcCall(rf.peers[server], "Raft.AppendEntry", args, reply)
}

// -------- RPC Handler ----------

// RequestVote RPC handler.
func (rf *Raft) RequestVote(arg *RequestVoteArgs, reply *RequestVoteReply) {
	rf.handlerTimerMutex.Lock()
	defer func() {
		rf.handlerTimerMutex.Unlock()
		rf.CurrentTerm.Store(maxInt32(rf.CurrentTerm.Load(), arg.Term))
		reply.Base.ToNodeID = arg.Base.FromNodeID
		reply.Base.FromNodeID = rf.me
		reply.Term = maxInt32(rf.CurrentTerm.Load(), arg.Term)
	}()

	if !isRaftAbleToGrantVote(arg, rf) {
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.voteFor.Store(int32(arg.Base.FromNodeID))
}

// AppendEntry RPC Handler
func (rf *Raft) AppendEntry(arg *AppendEntryArgs, reply *AppendEntryReply) {
	rf.handlerTimerMutex.Lock()
	defer rf.handlerTimerMutex.Unlock()

	reply.Base.FromNodeID = rf.me
	reply.Base.ToNodeID = arg.Base.FromNodeID
	from, _ := arg.GetAllCaseByUserID()
	reply.Term = rf.CurrentTerm.Load()

	if arg.Term < (rf.CurrentTerm.Load()) {
		reply.Success = false
		return
	}

	// force outdated leader to become follower
	if rf.state.Load().(State) != Follower && rf.me != arg.Base.FromNodeID {
		rf.becomeFollower(int32(from))
	}
	rf.CurrentTerm.Store(int32(arg.Term))
	rf.LeaderID.Store(int32(from))
	rf.voteFor.Store(int32(from))
	reply.Term = rf.CurrentTerm.Load()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// directly reply success local sent message
	if rf.me == arg.Base.FromNodeID {
		reply.Success = true
		reply.LastMatchIndex = len(rf.LocalLog) - 1
		return
	}

	freshEntries := rf.removeDuplicateLogsInArg(arg.Entries)

	// check if there's no log in prevLogIndex or prevLogIndex has different term
	// notice: this will set reply field to tell leader to fall back lastMatchIndex
	missing := rf.isLogMissing(arg.PrevLogIndex, arg.PrevLogTerm, reply)
	if missing {
		return
	}

	conflict, lastMatchIndex := rf.isLogConflict(arg.PrevLogIndex, arg.Entries)
	if conflict {
		// delete conflict logs and append leader's logs
		rf.LocalLog = rf.LocalLog[:lastMatchIndex+1]
	}
	// append leader's logs into follower's logs
	rf.LocalLog = append(rf.LocalLog, freshEntries...)

	// set node's commitIndex
	if arg.LeaderCommit > rf.CommitIndex.Load() {
		rf.CommitIndex.Store(minInt32(arg.LeaderCommit, int32(len(rf.LocalLog)-1)))
	}
	debug.Debug(debug.DLog, "S%d append %v into local log, log length:%d, now commitIndex: %d \n", rf.me, freshEntries, len(rf.LocalLog), rf.CommitIndex.Load())

	computeLastIndex := func() int {
		//if len(freshEntries) == 0 {
		//	return lastMatchIndex + len(arg.Entries)
		//}
		return len(rf.LocalLog) - 1
	}
	reply.LastMatchIndex = computeLastIndex()
	reply.Success = true
}

// -------- utils ---------

// remove logs which is already in the local logs
func (rf *Raft) removeDuplicateLogsInArg(appendingLogs []Log) []Log {
	freshEntries := make([]Log, 0)
	for _, log := range appendingLogs {
		isDuplicate := false
		for _, myLog := range rf.LocalLog {
			if log.Command == myLog.Command && log.Term == myLog.Term {
				isDuplicate = true
			}
		}
		if !isDuplicate {
			freshEntries = append(freshEntries, log)
		}
	}
	return freshEntries
}

func (rf *Raft) becomeLeader() {
	rf.LeaderID.Store(int32(rf.me))
	rf.initNextAndMatch()
	go rf.heartBeat()
	rf.state.Store(Leader)
	debug.Debug(debug.DInfo, "S%d becomes Leader in Term %d \n", rf.me, rf.CurrentTerm.Load())
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
	rf.state.Store(Candidate)
	debug.Debug(debug.DInfo, "S%d becomes Candidate in Term %d \n", rf.me, rf.CurrentTerm.Load())
}

func (rf *Raft) becomeFollower(leaderID int32) {
	clearLeaderState := func() {
		clearSyncMap(&rf.NextIndex)
		clearSyncMap(&rf.MatchIndex)
		clearSyncMap(&rf.receivedClientRequests)
	}
	rf.voteFor.Store(-1)
	rf.VotesFromPeers.Store(0)
	rf.LeaderID.Store(leaderID)
	rf.state.Store(Follower)
	clearLeaderState()
	debug.Debug(debug.DInfo, "S%d becomes Follower in Term %d \n", rf.me, rf.CurrentTerm.Load())
}

func (rf *Raft) isLeader() bool {
	return int(rf.LeaderID.Load()) == rf.me && rf.state.Load() == Leader
}

func (rf *Raft) buildAppendEntry(nextIndex int) (int, int32, []Log) {
	if nextIndex < 1 {
		return 0, 0, []Log{}
	}

	getAppendingLogs := func(nextIndex int, logs []Log) []Log {
		if nextIndex >= len(logs) {
			return []Log{}
		}
		return logs[nextIndex:]
	}
	rf.mu.Lock()
	sendingLogs := getAppendingLogs(nextIndex, rf.LocalLog)
	prevLogTerm := rf.LocalLog[nextIndex-1].Term
	rf.mu.Unlock()
	return nextIndex - 1, prevLogTerm, sendingLogs
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
			rf.sendRequestVote(server, req, reply)
			rf.processRequestVoteReply(*reply)
		}(i)
	}
}

func (rf *Raft) processRequestVoteReply(reply RequestVoteReply) {
	if reply.Term == rf.CurrentTerm.Load() && reply.VoteGranted {
		rf.VotesFromPeers.Add(1)
		if rf.VotesFromPeers.Load() == int32((len(rf.peers)+1)/2) {
			rf.becomeLeader()
		}
	}
}

// PreProcessLog Do some checks before apply to command to local log
// e.g: whether the command has already been processed
func (rf *Raft) PreProcessLog(command interface{}) (int, int32, bool) {
	debug.Debug(debug.DTrace, "S%d Receive %v from client, \n", rf.me, command)
	// just discard this message
	if !rf.isLeader() {
		return -1, -1, false
	}

	// check if the log already processed
	if val, ok := rf.receivedClientRequests.Load(command); ok {
		return val.(AlreadyApplyLog).index, val.(AlreadyApplyLog).term, true
	}

	// leader appends the log locally
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.LocalLog = append(rf.LocalLog, Log{
		Term:    rf.CurrentTerm.Load(),
		Command: command,
	})

	// put the log into local log
	index := len(rf.LocalLog) - 1
	rf.receivedClientRequests.Store(rf.LocalLog[index].Command, AlreadyApplyLog{
		index: index,
		term:  rf.CurrentTerm.Load(),
	})
	debug.Debug(debug.DLeader, "S%d, put %v into index: %d \n", rf.me, command, index)
	return index, rf.CurrentTerm.Load(), true
}

func (rf *Raft) processSuccessAppendReply(reply AppendEntryReply, matchIndex, server int) {
	// follower's term is larger than leader's term, which means that the leader is out of date
	if reply.Term > (rf.CurrentTerm.Load()) {
		rf.becomeFollower(-1)
		return
	}

	rf.MatchIndex.Store(server, matchIndex)
	rf.NextIndex.Store(server, matchIndex+1)
	debug.Debug(debug.DLog, "S%d increase S%d nextIndex:%d, matchIndex to: %d \n", rf.me, server, matchIndex+1, matchIndex)

	// check if leader's commitIndex can be increased
	rf.mu.Lock()
	n := len(rf.LocalLog)
	for i := n - 1; i > int(rf.CommitIndex.Load()); i-- {
		cnt := 0
		rf.MatchIndex.Range(func(key, value any) bool {
			if value.(int) >= i {
				cnt++
			}
			return true
		})
		if cnt >= (len(rf.peers)+1)/2 {
			rf.CommitIndex.Store(int32(i))
			debug.Debug(debug.DLog, "S%d increase committedIndex to:%d \n", rf.me, i)
			break
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) processFailAppendReply(reply AppendEntryReply, prevLogIndex int, server int) {
	computeNextIndex := func(reply AppendEntryReply, rf *Raft, server int) int {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		potentialMatchedIndex := reply.LastMatchIndex
		conflictTerm := reply.Term
		val, ok := rf.NextIndex.Load(server)
		if !ok {
			debug.Debug(debug.DError, "S%d GetNextIndex in Log failed, to S%d \n", rf.me, server)
			panic(&rf.NextIndex)
		}
		currentNextIndex := val.(int) - 1
		for currentNextIndex > potentialMatchedIndex {
			if rf.LocalLog[currentNextIndex].Term != int32(conflictTerm) {
				break
			}
			currentNextIndex--
		}
		return currentNextIndex + 1
	}

	// current node is the outdated leader
	if reply.Term > rf.CurrentTerm.Load() {
		rf.becomeFollower(-1)
		return
	}

	// the log entry is missing and move back the next index pointer
	if reply.LastMatchIndex < prevLogIndex {
		nextIndex := computeNextIndex(reply, rf, server)
		rf.NextIndex.Store(server, nextIndex)
		debug.Debug(debug.DLeader, "S%d set nextIndex: %d for S%d \n", rf.me, nextIndex, server)
	}

	return
}

// SyncLogs Leader send local logs based on the nextIndex to followers
func (rf *Raft) SyncLogs(currentTerm int32) {
	isLeaderDisconnected := func(reply AppendEntryReply) bool {
		if reply.Term != (currentTerm) {
			return false
		}

		server := reply.Base.FromNodeID
		val, ok := rf.failedRPCCounter.Load(server)
		if !ok {
			rf.failedRPCCounter.Store(server, 1)
			return false
		}
		rf.failedRPCCounter.Store(currentTerm, val.(int)+1)

		disconnectedServerCnt := 0
		rf.failedRPCCounter.Range(func(key, value any) bool {
			disconnectedServerCnt += 1
			return true
		})
		if disconnectedServerCnt > (len(rf.peers)+1)/2 {
			return true
		}
		return false
	}

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
			prevLogIndex, prevLogTerm, logEntries := rf.buildAppendEntry(nextIndex.(int))
			ok = rf.sendAppendEntry(server, &AppendEntryArgs{
				Term:         currentTerm,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.CommitIndex.Load(),
				Entries:      logEntries,
				Base: Base{
					FromNodeID: rf.me,
					ToNodeID:   server,
				},
			}, &reply)
			if !ok {
				debug.Debug(debug.DError, "S%d AppendEntry failed, to S%d \n", rf.me, server)
				if isLeaderDisconnected(reply) {
					rf.becomeFollower(-1)
				}
				return
			}
			clearSyncMap(&rf.failedRPCCounter)
			// break if the Node is not leader
			if !rf.isLeader() {
				return
			}
			// there might be some conflict or missing log in the follower
			if !reply.Success {
				rf.processFailAppendReply(reply, prevLogIndex, server)
				return
			}
			rf.processSuccessAppendReply(reply, prevLogIndex+len(logEntries), server)
		}(i)
	}
}

func (rf *Raft) flushLocalLog(log []Log, commitIndex int) {
	for i := int(rf.LastAppliedIndex.Load()) + 1; i <= commitIndex; i++ {
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      log[i].Command,
			CommandIndex: i,
		}
		rf.LastAppliedIndex.Add(1)
		debug.Debug(debug.DLog2, "S%d apply log: (command:%v, i: %d) finished! \n", rf.me, log[i].Command, i)
	}
}

func (rf *Raft) isLogMissing(prevLogIndex int, prevLogTerm int32, reply *AppendEntryReply) bool {
	if prevLogIndex >= len(rf.LocalLog) {
		reply.Term = prevLogTerm
		reply.LastMatchIndex = len(rf.LocalLog) - 1
		reply.Success = false
		return true
	}
	if rf.LocalLog[prevLogIndex].Term == prevLogTerm {
		return false
	}
	conflictTerm := rf.LocalLog[prevLogIndex].Term
	potentialMatchedIndex := prevLogIndex
	for potentialMatchedIndex = prevLogIndex; potentialMatchedIndex > 0; potentialMatchedIndex-- {
		if rf.LocalLog[potentialMatchedIndex].Term != conflictTerm {
			break
		}
	}
	reply.Term = prevLogTerm
	reply.LastMatchIndex = potentialMatchedIndex
	reply.Success = false
	return true
}

func (rf *Raft) isLogConflict(prevLogIndex int, remoteLogs []Log) (isConflict bool, lastMatchIndex int) {
	// scenario: put new log entry into the local log
	if prevLogIndex == len(rf.LocalLog)-1 {
		return false, prevLogIndex
	}

	// scenario: scenario: partitioned leader may have some uncommitted logs
	localLogCopy := rf.LocalLog[prevLogIndex+1:]
	for i := 0; i < minInt(len(remoteLogs), len(localLogCopy)); i++ {
		if localLogCopy[i].Term != remoteLogs[i].Term {
			return true, i + prevLogIndex
		}
	}
	return false, prevLogIndex
}
