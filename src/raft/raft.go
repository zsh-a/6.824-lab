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

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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
}

// state of server
const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

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

	electionTimeout time.Time
	log             []Log
	state           int
	currentTerm     int
	votedFor        int // -1 for none
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int

	ApplyCh chan ApplyMsg
}

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	oldTerm := rf.currentTerm
	reply.Term = oldTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	n := len(rf.log)

	rf.currentTerm = args.Term
	if args.Term > oldTerm {
		rf.votedFor = -1
	}
	rf.state = FOLLOWER
	if rf.votedFor == -1 && (args.LastLogTerm > rf.log[n-1].Term || (args.LastLogTerm == rf.log[n-1].Term && args.LastLogIndex >= n-1)) {
		rf.votedFor = args.CandidateId
		rf.electionTimeout = time.Now()
		//println("vote ", rf.me, "to", args.CandidateId)
		reply.VoteGranted = true
		return
	}
	// if rf.votedFor == -1 || (args.Term > oldTerm && (args.LastLogTerm > rf.log[n-1].Term || (args.LastLogTerm == rf.log[n-1].Term && args.LastLogIndex >= n-1))) {

	// }
	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	Term         int   //leader’s
	LeaderId     int   //so follower can redirect clients
	PrevLogIndex int   //index of log entry immediately preceding new ones
	PrevLogTerm  int   //term of prevLogIndex entry
	Entries      []Log //entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//println("notice", args.LeaderId, "to", rf.me)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.state = FOLLOWER
	n := len(rf.log) - 1
	rf.electionTimeout = time.Now()
	if n < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// println("leader id", args.LeaderId, "to ", rf.me)
	i, j := args.PrevLogIndex+1, 0
	for ; i < n && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			rf.log = rf.log[0:i]
			break
		}
	}

	for ; j < len(args.Entries); j++ {
		log.Println("id ", rf.me, " term ", rf.currentTerm, " command ", args.Entries[j].Command)
		rf.log = append(rf.log, args.Entries[j])
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(len(rf.log)-1, args.LeaderCommit)
		log.Println("id ", rf.me, " term ", rf.currentTerm, " commit ", rf.commitIndex)
	}

	rf.currentTerm = args.Term

	reply.Success = true
	if len(args.Entries) == 0 {
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			log.Println("id ", rf.me, "apply ", rf.lastApplied)
			rf.ApplyCh <- ApplyMsg{
				Command:      rf.log[rf.lastApplied].Command,
				CommandValid: true,
				CommandIndex: rf.lastApplied,
			}
		}
	}
	// for _, v := range rf.log {
	// 	print("id ", rf.me, " ", v.Command)
	// }
	// println()
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	}
	n := len(rf.log)
	rf.log = append(rf.log, Log{Term: rf.currentTerm, Index: n, Command: command})
	fmt.Println(command)
	return n, rf.currentTerm, true
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

func (rf *Raft) leaderElectionTimeout() {
	timeout := rand.Int63n(150) + 150
	for {
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			continue
		}

		if time.Since(rf.electionTimeout).Milliseconds() > timeout {

			// start leader election
			rf.state = CANDIDATE
			rf.currentTerm++
			rf.votedFor = rf.me
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
				LastLogIndex: len(rf.log) - 1,
			}
			rf.mu.Unlock()
			var vote int32 = 1
			for i := 0; i < len(rf.peers); i++ {
				if rf.me == i {
					continue
				}
				go func(idx int, arg RequestVoteArgs) {

					reply := RequestVoteReply{}
					rf.sendRequestVote(idx, &arg, &reply)
					if reply.VoteGranted {
						atomic.AddInt32(&vote, 1)
						println("vote ", idx, "to", rf.me)
					}
				}(i, args)
			}
			time.Sleep(50 * time.Millisecond)
			rf.mu.Lock()

			n := int32(len(rf.peers))
			rf.mu.Unlock()

			if atomic.LoadInt32(&vote) > n/2 {
				rf.mu.Lock()
				rf.state = LEADER
				log.Println("term ", rf.currentTerm, "leader ", rf.me)
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
				}
				rf.mu.Unlock()
			}
			timeout = rand.Int63n(150) + 150
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartbeat() {
	for {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}
		n := len(rf.log) - 1
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}
			args.PrevLogIndex = rf.nextIndex[i] - 1

			// println(i, rf.nextIndex[i])
			args.Entries = nil
			args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].Term
			if n >= rf.nextIndex[i] {
				args.Entries = rf.log[rf.nextIndex[i]:]
			}
			go func(idx int, arg AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(idx, &arg, &reply)

				rf.mu.Lock()
				if reply.Success {
					rf.nextIndex[idx] += len(arg.Entries)
					rf.matchIndex[idx] = arg.PrevLogIndex + len(arg.Entries)
					//println(idx, "next idx", rf.nextIndex[idx], "len ", len(arg.Entries), "n ", n)
				} else {
					if reply.Term == rf.currentTerm && rf.nextIndex[idx] > 1 {
						rf.nextIndex[idx]--
					}
				}
				rf.mu.Unlock()
			}(i, args)

		}
		rf.mu.Unlock()

		time.Sleep(20 * time.Millisecond)

		rf.mu.Lock()
		for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
			if rf.log[i].Term != rf.currentTerm {
				continue
			}
			cnt := 1
			for j := 0; j < len(rf.peers); j++ {
				if rf.me == j {
					continue
				}
				if rf.matchIndex[j] >= i {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = i
				log.Println("commit index : ", i, " command : ", rf.log[i].Command)
				break
			}
		}
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			log.Println("apply ", rf.lastApplied)
			rf.ApplyCh <- ApplyMsg{
				Command:      rf.log[rf.lastApplied].Command,
				CommandValid: true,
				CommandIndex: rf.lastApplied,
			}
		}
		rf.mu.Unlock()

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FOLLOWER
	rf.electionTimeout = time.Now()
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	n := len(peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	rf.ApplyCh = applyCh // notice : all of raft created by Make, they share global variabl,so can not use global variable
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = 1
	}
	go rf.leaderElectionTimeout()
	go rf.heartbeat()
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)
	return rf
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
