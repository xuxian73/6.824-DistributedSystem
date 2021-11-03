package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Dentry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the D each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	//"fmt"
	//"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Role int64

const (
	Follower Role = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive Dentries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Dentry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// Log entry struct
//
type LogEntry struct {
	Term    uint
	Command string
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm uint       // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log        	[]LogEntry // first index is 1
	commitIndex uint       // index of highest Dentry known to be commited
	lastApplied uint       // index of highest Dentry applied to state machine
	nextIndex   []uint     // for each server, index of the next Dentry to send to
	matchIndex  []uint     // for each server, index of highest Dentry known to be replicated

	role            Role       // the role of this server
	election_timer  *time.Timer // used for trigger a new round of election
	heartbeat_timer *time.Timer // used for trigger a new round of heartbeat
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Dthrough (and including)
// that index. Raft should now trim its Das much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint
	CandidateId  int
	LastLogIndex int
	LastLogTerm  uint
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint
	LeaderId     int
	PrevLogIndex uint
	PrevLogTerm  uint
	Entries      []LogEntry
	LeaderCommit uint
}

type AppendEntriesReply struct {
	Term    uint
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId){
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.set_follower()
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	
	if args.LastLogTerm >= rf.last_log_term() ||
		(args.LastLogIndex >= rf.last_log_index() &&
			args.LastLogTerm == rf.last_log_term()) {
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("%d not vote for %d", rf.me, args.CandidateId)
		return
	}
	DPrintf("%d vote for %d", rf.me, args.CandidateId)
	rf.votedFor = args.CandidateId
	rf.election_timer.Reset(rf.election_start_t_gen())
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		rf.set_follower()
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.Success = true
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the tar server in rf.peers[].
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's D if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft D since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.election_timer.C:
			rf.mu.Lock()
			rf.set_candidate()
			rf.election()
			rf.election_timer.Reset(rf.election_start_t_gen())
			rf.mu.Unlock()
		case <-rf.heartbeat_timer.C:
			rf.mu.Lock()
			if rf.role == Leader {
				rf.heartbeats()
				rf.heartbeat_timer.Reset(rf.heartbeat_interval())
			}
			rf.mu.Unlock()
		}
	}
	DPrintf("%d stop ticking", rf.me)
}

func (rf *Raft) election() {
	DPrintf("%d raise election", rf.me)
	vote_count := 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.last_log_index(),
		LastLogTerm:  rf.last_log_term(),
	}
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := RequestVoteReply{}
				DPrintf("%d send request vote to %d", rf.me, i)
				rf.sendRequestVote(i, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.role == Candidate {
					if reply.Term > rf.currentTerm {
						rf.set_follower()
						rf.currentTerm = reply.Term
					} else if reply.VoteGranted {
						vote_count++
						if vote_count > len(rf.peers)/2 {
							rf.set_leader()
							rf.heartbeats()
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) heartbeats() {
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := AppendEntriesReply{}
				DPrintf("%d send append entries to %d", rf.me, i)
				rf.sendAppendEntries(i, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.set_follower()
					rf.currentTerm = reply.Term
				} else if reply.Success {

					return
				}
			}(i)
		}
	}
}

func (rf *Raft) heartbeat_interval() time.Duration {
	return time.Duration(100) * time.Millisecond
}

// generate election_start_t from 200ms to 350ms
func (rf *Raft) election_start_t_gen() time.Duration {
	return time.Duration(rand.Int63n(150)+200) * time.Millisecond
}

// generate election_timeout_t from 200ms to 350ms
// func (rf *Raft) election_timeout_t_gen() time.Duration {
// 	return time.Duration(rand.Int63n(200) + 150) * time.Millisecond
// }

// last Dindex, 0 if empty
func (rf *Raft) last_log_index() int {
	return len(rf.log)
}

func (rf *Raft) last_log_term() uint {
	len := len(rf.log)
	if len > 0 {
		return rf.log[len-1].Term
	} else {
		return 0
	}
}

func (rf *Raft) set_follower() {
	DPrintf("%d to follower", rf.me)
	rf.role = Follower
	rf.votedFor = -1
	rf.heartbeat_timer.Stop()
	rf.election_timer.Reset(rf.election_start_t_gen())
}

func (rf *Raft) set_candidate() {
	DPrintf("%d to candidate", rf.me)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.role = Candidate
	rf.election_timer.Reset(rf.election_start_t_gen())
}

func (rf *Raft) set_leader() {
	DPrintf("%d to leader", rf.me)
	rf.role = Leader
	rf.election_timer.Stop()
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = uint(rf.last_log_index() + 1)
	}
	rf.heartbeat_timer.Reset(rf.heartbeat_interval())
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
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
	}

	// Your initialization code here (2A, 2B, 2C).
	
	rf.currentTerm = 0
	rf.role = Follower
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = Follower
	rf.nextIndex = make([]uint, len(peers))
	rf.matchIndex = make([]uint, len(peers))
	rf.election_timer = time.NewTimer(rf.election_start_t_gen())
	rf.heartbeat_timer = time.NewTimer(rf.heartbeat_interval())
	if !rf.heartbeat_timer.Stop() {
		DPrintf("failed")
	}
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("%d start ticking...", rf.me)
	
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
