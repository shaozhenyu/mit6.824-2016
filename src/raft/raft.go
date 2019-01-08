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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

type RaftState int

const (
	Leader RaftState = iota + 1
	Candidate
	Follower
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	term  int
	index int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool // true : candidate win this server's vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("recv RequestVote server(%d) state(%d)", rf.me, rf.state)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	lastLog := rf.logs[lastLogIndex(rf.logs)]
	if (lastLog.term < args.LastLogTerm) || (lastLog.term == args.LastLogTerm && lastLog.index <= args.LastLogIndex) {
		reply.VoteGranted = true
	}
	return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// PrevLogIndex int
	// PrevLogTerm  int
	// Entries      []*LogEntry
	// LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// send by leader for heartbeat and sync logs
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("AppendEntries server(%d), state(%d) term(%d) %v", rf.me, rf.state, args.Term, args)
	rf.recvLeader <- struct{}{}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertFollower(args.Term)
		reply.Success = true
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persist on all server
	currentTerm int
	votedFor    int
	logs        []*LogEntry

	// change frequently on all server
	commitIndex int
	lastApplied int

	// change frequently on leader
	nextIndex  []*LogEntry
	matchIndex []*LogEntry

	state      RaftState     // raft server state
	recvLeader chan struct{} // recv leader's requset
	voteNum    int
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

	// Your initialization code here.
	rf.init()
	go rf.run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) run() {
	DPrintf("Run server(%d), state(%d)", rf.me, rf.state)
	for {
		switch rf.state {
		case Follower:
			rf.doFollower()
		case Candidate:
			rf.doCandidate()
		case Leader:
			rf.doLeader()
		}
	}
}

func (rf *Raft) init() {
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]*LogEntry, 1, 4096)
	rf.logs[0] = &LogEntry{term: -1, index: -1}

	rf.state = Follower
	rf.recvLeader = make(chan struct{}, 1)
}

func (rf *Raft) doFollower() {
	// DPrintf("doFollower: server(%d), state(%d)", rf.me, rf.state)
	select {
	case <-electionTimeout(): // change to candidate
		rf.convertCandidate()
	case <-rf.recvLeader:
	}
}

func (rf *Raft) convertFollower(currentTerm int) {
	DPrintf("convertFollower newTerm (%d)", currentTerm)
	rf.state = Follower
	rf.currentTerm = currentTerm
	rf.votedFor = -1
	rf.voteNum = 0
}

func (rf *Raft) doCandidate() {
	DPrintf("doCandidate: server(%d), state(%d)", rf.me, rf.state)
	// check server state
	if rf.state != Candidate {
		return
	}

	// send request vote for all server
	reqVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs[lastLogIndex(rf.logs)].index,
		LastLogTerm:  rf.logs[lastLogIndex(rf.logs)].term,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.state != Candidate {
			return
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, reqVoteArgs, reply)
			if !ok {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.convertFollower(reply.Term)
				return
			}
			if rf.state != Candidate {
				return
			}
			rf.voteNum++
			// change to leader
			if rf.getMostVote() {
				rf.convertLeader()
				rf.recvLeader <- struct{}{}
				return
			}
		}(i)
	}

	select {
	case <-electionTimeout(): // change to candidate
		rf.convertCandidate()
	case <-rf.recvLeader:
	}
}

func (rf *Raft) getMostVote() bool {
	if rf.voteNum >= len(rf.peers)/2+1 {
		return true
	}
	return false
}

func (rf *Raft) convertCandidate() {
	DPrintf("convertCandidate: server(%d), state(%d)", rf.me, rf.state)
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.voteNum = 1
	rf.currentTerm++
}

func (rf *Raft) doLeader() {
	DPrintf("doLeader: server(%d), state(%d), term(%d)", rf.me, rf.state, rf.currentTerm)
	for {
		if rf.state != Leader {
			return
		}
		go func() {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.state != Leader {
					break
				}

				go func(server int) {
					args := AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					reply := &AppendEntriesReply{}
					success := rf.sendAppendEntries(server, args, reply)
					if !success {
						return
					}
				}(i)
			}
		}()
		<-broadcastTime()
	}
}

func (rf *Raft) convertLeader() {
	DPrintf("convertLeader server(%d)", rf.me)
	rf.state = Leader
	rf.votedFor = rf.me
	rf.voteNum = 0
}

// heartbeat
func electionTimeout() <-chan time.Time {
	return time.After(time.Duration(100+rand.Int63()%400) * time.Millisecond)
}

func broadcastTime() <-chan time.Time {
	return time.After(10 * time.Millisecond)
}

func lastLogIndex(logs []*LogEntry) int {
	return len(logs) - 1
}
