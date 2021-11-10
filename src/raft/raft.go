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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	Command interface{}
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
	currentTerm     uint       // latest term server has seen
	votedFor        int        // candidateId that received vote in current term
	log             []LogEntry // first index is 1
	commitIndex     uint       // index of highest Dentry known to be commited
	lastApplied     uint       // index of highest Dentry applied to state machine
	nextIndex       []uint     // for each server, index of the next Dentry to send to
	matchIndex      []uint     // for each server, index of highest Dentry known to be replicated
	applyCh         chan ApplyMsg
	applyCond       *sync.Cond
	role            Role        // the role of this server
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	var currentTerm uint
	var votedFor int
	var logs []LogEntry

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("decode error")
	} else {
		rf.currentTerm, rf.votedFor, rf.log = uint(currentTerm), votedFor, logs
	}

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
	Term          uint
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.set_follower()
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	if args.LastLogTerm > rf.last_log_term() ||
		(args.LastLogIndex >= rf.last_log_index() &&
			args.LastLogTerm == rf.last_log_term()) {
		DPrintf("%d receive %d's request vote at term %d, [LastLogTerm: %d], [LastLogIndex: %d]", rf.me, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex)
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("%d not vote for %d", rf.me, args.CandidateId)
		return
	}
	DPrintf("%d vote for %d at term %d", rf.me, args.CandidateId, rf.currentTerm)
	rf.votedFor = args.CandidateId
	rf.election_timer.Reset(rf.election_start_t_gen())
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success, reply.ConflictIndex = rf.currentTerm, false, -1
		DPrintf("%d reject append entry of %d for old term", rf.me, args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.set_follower()
	rf.currentTerm = args.Term
	// prev log matched
	if args.PrevLogIndex == 0 ||
		(uint(len(rf.log)) >= args.PrevLogIndex && rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm) {
		i, j := 0, 0
		for i = int(args.PrevLogIndex); j < len(args.Entries); j += 1 {
			if i >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[j])
			} else {
				rf.log[i] = args.Entries[j]
			}
			i += 1
		}
		if i < len(rf.log) {
			rf.log = rf.log[0:i]
		}
		DPrintf("%d accept append entry of %d, log size %d", rf.me, args.LeaderId, len(rf.log))
		//DPrintf("%v", rf.log)
		reply.Term, reply.Success, reply.ConflictIndex = args.Term, true, -1
		rf.advanceCommitIndex_follower(int(args.LeaderCommit))
	} else {
		if args.PrevLogIndex > uint(len(rf.log)) {
			reply.ConflictIndex, reply.ConflictTerm = len(rf.log)+1, -1
		} else {
			reply.ConflictIndex, reply.ConflictTerm = int(args.PrevLogIndex), int(rf.log[args.PrevLogIndex-1].Term)
			for reply.ConflictIndex >= 1 && int(rf.log[reply.ConflictIndex-1].Term) == reply.ConflictTerm {
				reply.ConflictIndex -= 1
			}
			reply.ConflictIndex += 1
		}

		DPrintf("%d reject append entry of %d for mismatched log, PrevLog[index: %d, term: %d], ConflictIndex: %d, ConflictTerm: %d",
			rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, reply.ConflictIndex, reply.ConflictTerm)
		reply.Term, reply.Success = rf.currentTerm, false
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.role == Leader
	if !isLeader {
		return -1, -1, false
	}
	rf.applyEntry(command)
	index = rf.last_log_index()
	term = int(rf.currentTerm)
	DPrintf("%d receive command [%d] at term %d", rf.me, index, term)
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
	DPrintf("%d is killed", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartbeat_interval() time.Duration {
	return time.Duration(100) * time.Millisecond
}

// generate election_start_t from 200ms to 350ms
func (rf *Raft) election_start_t_gen() time.Duration {
	return time.Duration(rand.Int63n(150)+200) * time.Millisecond
}

func (rf *Raft) last_log() *LogEntry {
	return &rf.log[len(rf.log)-1]
}

// generate appendentries args
func (rf *Raft) append_entry_args_gen(peer int) (AppendEntriesArgs, bool) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogIndex, args.PrevLogTerm = rf.prev_log_index_and_term(peer)
	for i := rf.nextIndex[peer] - 1; i < uint(len(rf.log)); i += 1 {
		args.Entries = append(args.Entries, rf.log[i])
	}
	return args, len(args.Entries) == 0
}

func (rf *Raft) advanceCommitIndex() {
	matchIndex := make([]uint, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Slice(matchIndex, func(i, j int) bool {
		return matchIndex[i] < matchIndex[j]
	})
	CommitIndex := matchIndex[len(matchIndex)/2]
	if CommitIndex > rf.commitIndex && rf.isLogFresh(rf.currentTerm, int(CommitIndex)) {
		rf.commitIndex = CommitIndex
		rf.applyCond.Signal()
		DPrintf("leader %d advance commitIndex to %d", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) advanceCommitIndex_follower(LeaderCommit int) {
	commitIndex := Min(LeaderCommit, len(rf.log))
	if rf.commitIndex < uint(commitIndex) {
		rf.commitIndex = uint(commitIndex)
		DPrintf("follower %d advance commitIndex to %d", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}
}

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

func (rf *Raft) prev_log_index_and_term(peer int) (uint, uint) {
	if rf.nextIndex[peer] > 1 {
		i := rf.nextIndex[peer] - 1
		return uint(i), rf.log[i-1].Term
	} else {
		return 0, 0
	}
}

func (rf *Raft) set_follower() {
	//DPrintf("%d to follower", rf.me)
	rf.role = Follower
	rf.heartbeat_timer.Stop()
	rf.election_timer.Reset(rf.election_start_t_gen())
}

func (rf *Raft) set_candidate() {
	//DPrintf("%d to candidate", rf.me)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.role = Candidate
	rf.election_timer.Reset(rf.election_start_t_gen())
}

func (rf *Raft) set_leader() {
	// DPrintf("%d to leader", rf.me)
	rf.role = Leader
	rf.election_timer.Stop()
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = uint(rf.last_log_index() + 1)
	}
	rf.matchIndex[rf.me] = uint(len(rf.log))
	rf.heartbeat_timer.Reset(rf.heartbeat_interval())
}

func (rf *Raft) applyEntry(command interface{}) {
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.matchIndex[rf.me] = uint(len(rf.log))
	//DPrintf("%v", rf.log)
	rf.persist()
}

func (rf *Raft) isLogFresh(term uint, index int) bool {
	return index <= rf.last_log_index() && rf.log[index-1].Term == term
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
			rf.persist()
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

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		for i := rf.lastApplied; i < rf.commitIndex; i += 1 {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: int(i + 1),
			}
			rf.lastApplied += 1
			DPrintf("%d apply command %d at term %d", rf.me, i+1, rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) election() {
	DPrintf("%d raise election at term %d", rf.me, rf.currentTerm)
	vote_count := 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.last_log_index(),
		LastLogTerm:  rf.last_log_term(),
	}
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				reply := RequestVoteReply{}
				DPrintf("%d send request vote to %d", rf.me, peer)
				rf.sendRequestVote(peer, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.role == Candidate && rf.currentTerm == args.Term {
					if reply.Term > rf.currentTerm {
						rf.set_follower()
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.persist()
					} else if reply.VoteGranted {
						vote_count++
						if vote_count > len(rf.peers)/2 {
							rf.set_leader()
							rf.persist()
							rf.heartbeats()
						}
					}
				}
			}(peer)
		}
	}
}

func (rf *Raft) heartbeats() {
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}
				args, empty := rf.append_entry_args_gen(peer)
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				if empty {
					DPrintf("%d send heartbeat to %d, prev log [%d, %d]",
						rf.me, peer, args.PrevLogIndex, args.PrevLogTerm)
				} else {
					DPrintf("%d send append entries to %d at %d, prev log [%d, %d]",
						rf.me, peer, args.Term, args.PrevLogIndex, args.PrevLogTerm)
				}
				rf.sendAppendEntries(peer, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.role == Leader && rf.currentTerm == args.Term {
					if reply.Term > rf.currentTerm {
						rf.set_follower()
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.persist()
						return
					}
					if reply.Success {
						rf.matchIndex[peer] = args.PrevLogIndex + uint(len(args.Entries))
						rf.nextIndex[peer] = rf.matchIndex[peer] + 1
						rf.advanceCommitIndex()
					} else if reply.Term == rf.currentTerm {
						DPrintf("%d receive %d rejection, ConflictIndex %d", rf.me, peer, reply.ConflictIndex)
						rf.nextIndex[peer] = uint(reply.ConflictIndex)
						if reply.ConflictTerm != -1 {
							for i := args.PrevLogIndex; i > 0; i-- {
								if rf.log[i-1].Term == uint(reply.ConflictTerm) {
									rf.nextIndex[peer] = i + 1
									break
								}
							}
						}
					}

				}
			}(peer)
		}
	}
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
		applyCh:   applyCh,
		dead:      0,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCond = sync.NewCond(&rf.mu)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("%d persist: term %d, votedfor %d, logsize %d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = uint(rf.last_log_index() + 1)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = uint(len(rf.log))
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
