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
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "math/rand"
import "time"
import "bytes"
import "../labgob"
import "log"


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term int
}

type LogEntry struct {
	Index int
	Term int
	Command interface{}
}


type SnapshotState string

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Instance state, access requite holding instance mu.
	heartbeatReceived bool
	currentTerm int
	votedFor int
	isLeader bool

	// Id of leader this raft peer thinks, -1 if never set.
	leaderId int

	logs []LogEntry
	// Leader use this to queue new log for each followers
	logChans []chan int

	matchIndex []int

	commitIndex int
	lastApplied int

	applyCh chan ApplyMsg
	heartbeatChan chan int
	stepDownCh chan int

	lastIncludedIndex int
	lastIncludedTerm int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func Min(a, b int) int{
	if a > b {
		return b
	}
	return a
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) HeartbeatReceived() bool {
	rf.mu.Lock()
	re := rf.heartbeatReceived
	rf.mu.Unlock()
	return re
}

func (rf *Raft) StepDownCh() chan int{
	return rf.stepDownCh
}

func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	re := rf.isLeader
	rf.mu.Unlock()
	return re
}

func (rf *Raft) GetCurrentTerm() int {
	rf.mu.Lock()
	re := rf.currentTerm
	rf.mu.Unlock()
	return re
}

func (rf *Raft) logLen() int {
	re := len(rf.logs) + rf.lastIncludedIndex + 1
	return re
}

func (rf *Raft) getLog(index int) *LogEntry {
	if index <= rf.lastIncludedIndex {
		log.Fatal("trying to access log already witten to snapshot.")
	}
	re := &rf.logs[index - rf.lastIncludedIndex - 1]
	if re.Index != index {
		log.Fatal("log index mismatch.")
	}
	return re
}

func (rf *Raft) GetLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderId
}

// Get a random generated timeout in ms.
func GetTimeout(seed int64) int {
	base := 500
	random := rand.Int31() % 500
	return base + int(random)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader= rf.isLeader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	DPrintfToFile(rf.me,"%d persisted to index %d \n", rf.me, rf.logLen() - 1)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
	  DPrintfToFile(rf.me, "%d readPersist error\n", rf.me)
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.logs = logs
	  rf.lastIncludedIndex = lastIncludedIndex
	  rf.lastIncludedTerm = lastIncludedTerm
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	DPrintfToFile(rf.me, "raft:%d attempt lock %d \n", rf.me, 1)
	rf.mu.Lock()
	DPrintfToFile(rf.me, "raft:%d lock %d \n", rf.me, 1)
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// do not grant vote and ask caller to update term.
		reply.VoteGranted = false
	} else {
		// increment term if know of a higher term.
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
			// Convert to follower if this instance is leader.
			if rf.isLeader {
				rf.StepDown()
			}
		}
		// vote for this candidate if we can.
		if rf.ShouldGrantVote(args.LastLogIndex, args.LastLogTerm) {
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
		}
	}
	// always reply the current term.
	reply.Term = rf.currentTerm
	DPrintfToFile(rf.me, "%d vote for %d for term %d result: %t", rf.me, args.CandidateId, args.Term, reply.VoteGranted)
	DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 1)
}

func (rf *Raft) ShouldGrantVote(lastIndex int, lastTerm int) bool {
	if rf.votedFor != -1 {
		return false
	}
	t := rf.lastIncludedTerm
	if len(rf.logs) > 0 {
		t = rf.logs[len(rf.logs) - 1].Term
	}
	// compare term first
	if lastTerm > t {
		return true
	}
	if lastTerm < t {
		return false
	}

	index := rf.lastIncludedIndex
	if len(rf.logs) > 0 {
		index = rf.logs[len(rf.logs) - 1].Index
	}
	// compare index second if term are same.
	return lastIndex >= index
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Leader's term who calls AppendEntries.
	Term int
	// used to redirect clients.
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Receiver's term for caller to possibly correct itself.
	Term int
	Success bool

	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintfToFile(rf.me, "raft:%d attempt lock %d \n", rf.me, 2)
	rf.mu.Lock()
	//DPrintfToFile(rf.me, "raft:%d lock %d \n", rf.me, 2)
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		// request is valid, so set heartbeat called.
		rf.heartbeatReceived = true
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		if rf.isLeader {
			rf.StepDown()
		}
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// one term cannot have 2 leaders, I think i'am
	if args.Term == rf.currentTerm && rf.isLeader {
		reply.Success = false
		return
	}

	// Now, there is nothing stoping us believe caller is leader, so update leaderId.
	rf.leaderId = args.LeaderId

	// Maybe append this log if it's not a heartbeat.
	if len(args.Entries) > 0 {
		// Compare prev log if exists.
		if args.PrevLogIndex >= 0 {
			if args.PrevLogIndex >= rf.logLen() {
				reply.Success = false
				reply.NextIndex = rf.logLen()
				return
			}

			if args.PrevLogIndex == rf.lastIncludedIndex {
				if args.PrevLogTerm != rf.lastIncludedTerm {
					log.Fatal("AppendEntries received prev log mismatch with commited SnapShot.")
				}
			} else if args.PrevLogIndex < rf.lastIncludedIndex {
				log.Fatal("AppendEntries try to updated snapshot.")
			} else {
				if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
					reply.Success = false
					nextPrev := args.PrevLogIndex - 1
					for nextPrev >= 0 && rf.logs[nextPrev].Term ==  rf.logs[args.PrevLogIndex].Term {
						nextPrev--
					}
					reply.NextIndex = nextPrev + 1
					return
				}
			}
		}
		rf.logs = rf.logs[0: args.PrevLogIndex + 1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}
	reply.Success = true

	// update commit status from leader
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = args.LeaderCommit
		rf.ApplyToN(rf.commitIndex)
	}
	//DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 2)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintfToFile(rf.me, "raft:%d attempt lock %d \n", rf.me, 0)
	rf.mu.Lock()
	DPrintfToFile(rf.me, "raft:%d lock %d \n", rf.me, 0)
	defer rf.mu.Unlock()
	// retrun early if this raft instance is not leader.
	if !rf.isLeader {
		DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 0)
		return -1, -1, false
	}
	DPrintfToFile(rf.me, "start called on %d \n", rf.me)
	// start with changing state of leader.

	newLog := LogEntry{Index: rf.logLen(), Term: rf.currentTerm, Command: command}
	term := rf.currentTerm
	rf.logs = append(rf.logs, newLog)
	rf.persist()
	index := rf.logLen() - 1
	
	// queue this log to logChan for each raft peer.
	for i,_ := range rf.peers {
		if i == rf.me { continue }
		DPrintfToFile(rf.me, "%d send %d to logchans %d\n", rf.me, index, i)
		rf.logChans[i] <- index
	}

	DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 0)
	return index , term, true
}

func (rf *Raft) UpdatePeerIterative(peer int, index int, term int) {
	if index < 0 {
		return
	}
	nextIndex := index
	for {
		if term != rf.GetCurrentTerm() || !rf.IsLeader()  || rf.killed(){
			return
		}
		success, retry, next := rf.Append(peer, nextIndex, index, term)
		if success { break }
		if !retry { return }
		nextIndex = next
	}

	DPrintfToFile(rf.me, "raft:%d attempt lock %d \n", rf.me, 3)
	rf.mu.Lock()
	DPrintfToFile(rf.me, "raft:%d lock %d \n", rf.me, 3)
	defer rf.mu.Unlock()

	if !rf.isLeader {
		return
	}
	// maybe update matchindex if it was lower.
	if rf.matchIndex[peer] < index {
		rf.matchIndex[peer] = index
	}

	// update commit if possible.
	if rf.CanCommit(index) && rf.commitIndex < index && rf.getLog(index).Term == rf.currentTerm {
		rf.commitIndex = index
		rf.ApplyToN(index)
	}
	DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 3)
}

func (rf *Raft) Append(peer, startIndex, endIndex, term int) (bool, bool, int) {
	DPrintfToFile(rf.me, "raft:%d attempt lock %d \n", rf.me, 4)
	rf.mu.Lock()
	DPrintfToFile(rf.me, "raft:%d lock %d \n", rf.me, 4)
	if !rf.isLeader || rf.currentTerm != term || rf.killed(){
		DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 4)
		return false, false, 0
	}
	request := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	// prepare request
	request.Term = term
	request.LeaderId = rf.me
	prevLogTerm := rf.lastIncludedTerm
	if startIndex > rf.lastIncludedIndex + 1 {
		prevLogTerm = rf.getLog(startIndex - 1).Term
	}
	request.PrevLogIndex = startIndex - 1
	request.PrevLogTerm = prevLogTerm
	request.Entries = rf.logs[startIndex - (rf.lastIncludedIndex + 1): endIndex + 1 - (rf.lastIncludedIndex + 1)]
	request.LeaderCommit = Min(rf.commitIndex, rf.matchIndex[peer])
	DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 4)
	rf.mu.Unlock()

	rpcSuccess := false
	for !rpcSuccess && rf.IsLeader() && !rf.killed(){
		//DPrintfToFile(rf.me, "%d send rpc applyonece to %d for index %d\n", rf.me, peer, prevLogIndex + 1)
		rpcSuccess = rf.sendAppendEntries(peer, &request, &reply)
		//DPrintfToFile(rf.me, "%d send rpc applyonece to %d for index done %d\n", rf.me, peer, prevLogIndex + 1)
	}
	
	// back to follower if reply term > term of this instance
	DPrintfToFile(rf.me, "raft:%d attempt lock %d \n", rf.me, 5)
	rf.mu.Lock()
	DPrintfToFile(rf.me, "raft:%d lock %d \n", rf.me, 5)
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		if rf.isLeader {
			rf.StepDown()
		}
		DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 5)
		return false, false, 0
	}
	DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 5)
	return reply.Success, true, reply.NextIndex
}

func (rf *Raft) CanCommit(index int) bool {
	cnt := 1
	for i,_ := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] >= index {
			cnt++
		}
	}
	return cnt > len(rf.peers)/2
}


func (rf *Raft) InitLeader() {
	// data structure reinit. hold lock when call this.
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logChans = make([]chan int, len(rf.peers))
	for i,_ := range rf.peers {
		if i == rf.me { continue }
		rf.matchIndex[i] = -1
		rf.logChans[i] = make(chan int, 50000)
	}

	startIndex := rf.logLen() - 1
	lStartTerm := rf.currentTerm

	for i,_ := range rf.peers {
		if i == rf.me { continue }
		// this will be the first msg in each channel, this make each peer to come up to date as leader.
		rf.logChans[i] <- startIndex
		// start routines to accept new message.
		go func (peer int, startTerm int) {
			for idx := range rf.logChans[peer] {
				if startTerm != rf.GetCurrentTerm() || !rf.IsLeader() || rf.killed(){
					DPrintfToFile(rf.me, "%d logchan exit because new term start, old term: %d, new term: %d", rf.me, startTerm, rf.GetCurrentTerm())
					break
				}
				DPrintfToFile(rf.me, "%d: logchans[%d] received %d\n", rf.me, peer, idx)
				rf.UpdatePeerIterative(peer, idx, startTerm)
			}
		}(i, lStartTerm)
	}
}

func (rf *Raft) StepDown() {
	rf.isLeader = false
	for i,_ := range rf.peers{
		if i == rf.me {
			continue
		}
		close(rf.logChans[i])
	}
	rf.stepDownCh <- 1
	DPrintfToFile(rf.me, "%d step down as leader", rf.me)
}

func (rf *Raft) StartElection() {
	DPrintfToFile(rf.me, "raft %d start election\n", rf.me)
	// Increment term, vote for self
	DPrintfToFile(rf.me, "raft:%d attempt lock %d \n", rf.me, 6)
	rf.mu.Lock()
	DPrintfToFile(rf.me, "raft:%d lock %d \n", rf.me, 6)
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.persist()
	// Record the term this election is for, if it's changed during the election, that means a higher term exists, so this election must be invalid.
	termVoting := rf.currentTerm

	// prepare request
	request := RequestVoteArgs{}
	request.Term = rf.currentTerm
	request.CandidateId = rf.me

	if len(rf.logs) > 0 {
		request.LastLogTerm = rf.logs[len(rf.logs) - 1].Term
	} else {
		request.LastLogTerm = rf.lastIncludedTerm
	}
	request.LastLogIndex = rf.logLen() - 1

	rf.mu.Unlock()
	DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 6)

	// send requests and count vote.

	// already voted for self at this point.
	vote := 1

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			// prepare reply for each rpc.
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &request, &reply)

			// Routines here need to hold state lock since we are updating instance state.
			DPrintfToFile(rf.me, "raft:%d attempt lock %d \n", rf.me, 7)
			rf.mu.Lock()
			DPrintfToFile(rf.me, "raft:%d lock %d \n", rf.me, 7)
			defer rf.mu.Unlock()
			if reply.VoteGranted {
				vote++
				if rf.currentTerm == termVoting && vote > len(rf.peers) / 2 && !rf.isLeader {
					// election succeded, this instance will be new leader.
					rf.isLeader = true
					DPrintfToFile(rf.me, "raft %d claimed leader for term : %d\n", rf.me, termVoting)
					// send out heartbeat as soon as leader elected.

					// init leader state
					rf.InitLeader()
					rf.heartbeatChan <- 1				
				}
			}

			// Update currentTerm if this raft instance now knows a higher term, also clear votedFor.
			// We must mark this election as fail since now this instance has a new term number.
			// This moves this instance to a new term and to a follower state.
			if reply.Term > rf.currentTerm {
				// this will make sure this instance won't become leader at the end of this function.
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
			}
			DPrintfToFile(rf.me, "raft:%d unlock %d \n", rf.me, 7)
		}(i)
	}
}

// This will apply current logs up to N or current max log index, make sure hold lock when calling this.
// no locking in this function to avoid double locking.
func (rf *Raft) ApplyToN(n int) {
	if n >= rf.logLen() {
		n = rf.logLen() - 1
	}
	for rf.lastApplied < n {
		rf.lastApplied++
		msg := ApplyMsg{CommandValid: true, Command: rf.getLog(rf.lastApplied).Command, CommandIndex: rf.lastApplied, Term: rf.getLog(rf.lastApplied).Term}
		DPrintfToFile(rf.me, "%d committed %d \n", rf.me, rf.lastApplied)
		rf.applyCh <- msg
	}
}

func (rf *Raft) SnapShot(index int, term int, snapshot []byte) {
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.logs = rf.logs[index + 1:]

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartbeatReceived = false
	rf.isLeader = false
	rf.heartbeatChan = make(chan int, 5)
	rf.stepDownCh = make(chan int, 5)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyCh = applyCh
	rf.leaderId = -1
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	// Your initialization code here (2A, 2B, 2C).

	// start go routine to periodically check heartbeat, if no heartbeat received, start election.
	go func() {
		for {
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			rf.heartbeatReceived = false
			rf.mu.Unlock()
			t := GetTimeout(int64(rf.me))
			time.Sleep(time.Duration(t) * time.Millisecond)
			if !rf.HeartbeatReceived() {
				// start election if this is not leader.
				if rf.IsLeader() {
					continue
				}
				rf.StartElection()
			}
		}
	}()

	// start a go routine to send heartbeat if is leader.
	go func() {
		for {
			if rf.killed() {
				break
			}

			timoutChan := make(chan int, 1)
			go func() {
				hbInterval := 100
				time.Sleep(time.Duration(hbInterval) * time.Millisecond)
				timoutChan <- 1
			}()
			// Wait until regualer timeout or heartbeatChan says we need to send it immediately.
			select {
			case <- timoutChan:
			case <- rf.heartbeatChan:
			}

			if !rf.IsLeader() {
				continue
			}

			rf.mu.Lock()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()

			// leader sends heartbeat
			// Do not wait for heartbeat to finish since it will sloww down sending hb.
			for i, _ := range rf.peers {
				if (i == rf.me) {
					continue
				}
				go func(server int){
					request := AppendEntriesArgs{}
					reply := AppendEntriesReply{}
					request.Term = currentTerm
					request.LeaderId = rf.me

					rf.mu.Lock()
					request.LeaderCommit = Min(rf.commitIndex, rf.matchIndex[server])
					rf.mu.Unlock()

					rf.sendAppendEntries(server, &request, &reply)
					// back to follower if reply term > term of this instance
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						if rf.isLeader {
							rf.StepDown()
						}
					}
				}(i)
			}
		}	
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
