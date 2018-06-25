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
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

const (
	Follower = iota + 1
	Candidate
	Leader
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
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
	currentTerm int
	votedFor    int
	votedNum    int
	logs        []*logEntries
	state       int

	// often change in all service
	commitIndex int
	lastApplied int

	// often change in leader (init when new election)
	nextIndex  []int
	matchIndex []int

	recvLeader chan struct{}
}

type logEntries struct {
	Index int
	Term  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
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
	// Your data here.
	Term         int
	CandidateId  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 转化为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		rf.convertFollower()
		rf.mu.Lock()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.logNewer(args) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.recvLeader <- struct{}{}
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

func (rf *Raft) logNewer(args RequestVoteArgs) bool {
	if args.lastLogTerm > rf.lastLogTerm() {
		return true
	}
	if args.lastLogTerm == rf.lastLogTerm() && args.lastLogIndex >= rf.lastLogIndex() {
		return true
	}
	return false
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

// 复制日志指令，heartbeat
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*logEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// append log rpc
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		rf.convertFollower()
		rf.mu.Lock()
	}
	rf.mu.Unlock()
	rf.recvLeader <- struct{}{}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, repy *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, repy)
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

func (rf *Raft) convertFollower() {
	fmt.Println("convertFollower: ", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) convertCandidate() {
	fmt.Println("convertCandidate: ", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votedNum = 1
}

func (rf *Raft) convertLeader() {
	fmt.Println("convertLeader: ", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Leader
	rf.votedFor = -1
}

func (rf *Raft) handleFollower() {
	select {
	case <-time.After(candidateTimeout()):
		rf.convertCandidate()
	case <-rf.recvLeader:
	}
}

func (rf *Raft) handleCandidate() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		requestVoteArgs := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			lastLogIndex: len(rf.logs) - 1,
			lastLogTerm:  rf.lastLogTerm(),
		}
		rf.mu.Unlock()

		go func(server int) {
			requestVoteReply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, requestVoteArgs, requestVoteReply)
			if ok {
				rf.mu.Lock()
				// 已经产生leader
				if requestVoteReply.Term > rf.currentTerm {
					rf.mu.Unlock()
					rf.convertFollower()
					rf.recvLeader <- struct{}{}
					return
				}
				// 获取选票失败
				if requestVoteReply.Term < rf.currentTerm || requestVoteReply.VoteGranted == false {
					rf.mu.Unlock()
					return
				}
				// 获取选票成功
				if requestVoteReply.Term == rf.currentTerm && requestVoteReply.VoteGranted == true {
					rf.votedNum += 1
					if rf.getMostServiceVote() {
						fmt.Println("success---------", rf.me, rf.votedNum)
						rf.mu.Unlock()
						rf.convertLeader()
						rf.recvLeader <- struct{}{}
						return
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}

	select {
	case <-time.After(candidateTimeout()):
	case <-rf.recvLeader:
	}
}

func (rf *Raft) handleLeader() {
	fmt.Println("do leader worker")
	// 重新初始化nextIndex和matchIndex
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		// send heartbeat
		go func() {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				rf.mu.Lock()
				// 准备存储的日志条目
				entries := make([]*logEntries, 0)
				nextIndex := rf.nextIndex[i]
				if nextIndex <= rf.lastLogIndex() {
					entries = rf.logs[nextIndex:]
				}

				appendEntriesArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.logs[nextIndex-1].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(i, appendEntriesArgs, reply)
				if ok {
				}
			}
		}()

		time.Sleep(heartBeatTime())
	}
}

func (rf *Raft) startWork() {
	for {
		rf.recvLeader = make(chan struct{}, 10)
		switch rf.state {
		case Follower:
			rf.handleFollower()
		case Candidate:
			rf.convertCandidate()
			rf.handleCandidate()
		case Leader:
			rf.handleLeader()
		default:
			log.Fatal("UNKNOWN Raft state")
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = Candidate
	rf.logs = make([]*logEntries, 1)
	rf.logs[0] = &logEntries{0, 0}
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.recvLeader = make(chan struct{}, 10)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// run
	go rf.startWork()

	return rf
}

func (rf *Raft) lastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getMostServiceVote() bool {
	return rf.votedNum >= len(rf.peers)/2+1
}

// 跟随者响应时间 50ms
func heartBeatTime() time.Duration {
	return 50 * time.Millisecond
}

// 选举超时时间 300-500ms
func candidateTimeout() time.Duration {
	return time.Duration(300+rand.Int63n(200)) * time.Millisecond
}
