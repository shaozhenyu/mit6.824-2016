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
	"bytes"
	"encoding/gob"
	// "fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

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
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("recv RequestVote server(%d) state(%d) term(%d) cid(%d)", rf.me, rf.state, args.Term, args.CandidateId)

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertFollower()
	}

	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := rf.lastLogIndex()
		lastLogTerm := rf.lastLogTerm()
		if (lastLogTerm < args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.recvLeader <- struct{}{}
		}
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// send by leader for heartbeat and sync logs
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("AppendEntries server(%d), state(%d) term(%d) %v", rf.me, rf.state, args.Term, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		reply.Term = rf.currentTerm
	}()

	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertFollower()
		reply.Success = true
	}

	// log not match
	if len(rf.logs) <= args.PrevLogIndex {
		reply.Success = false
		rf.recvLeader <- struct{}{}
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.recvLeader <- struct{}{}
		return
	}
	reply.Success = true
	// delete conflict log and add new log
	if len(args.Entries) > 0 {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}

	// update server commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.commitCh <- struct{}{}
	}

	rf.recvLeader <- struct{}{}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	isLeader := (rf.state == Leader)
	if isLeader {
		rf.logs = append(rf.logs, &LogEntry{
			Index:   rf.lastLogIndex() + 1,
			Term:    rf.currentTerm,
			Command: command,
		})
	}
	index := rf.lastLogIndex()
	term := rf.currentTerm
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
	nextIndex  []int
	matchIndex []int

	state      RaftState     // raft server state
	recvLeader chan struct{} // recv leader's requset
	commitCh   chan struct{} // recv commit
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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()
	go rf.sendMsg(applyCh)

	return rf
}

func (rf *Raft) sendMsg(applyCh chan ApplyMsg) {
	for range rf.commitCh {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			log := rf.logs[rf.lastApplied]
			applyCh <- ApplyMsg{
				Index:   log.Index,
				Command: log.Command,
			}
			DPrintf("applied: %d %v", rf.lastApplied, log.Command)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) run() {
	DPrintf("Run server(%d), state(%d)", rf.me, rf.state)
	rf.recvLeader = make(chan struct{}, 10)
	for {
		switch rf.state {
		case Follower:
			rf.doFollower()
		case Candidate:
			rf.doCandidate()
		case Leader:
			rf.doLeader()
		default:
			panic("unknown raft state")
		}
	}
}

func (rf *Raft) init() {
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]*LogEntry, 1, 4096)
	rf.logs[0] = &LogEntry{Term: 0, Index: 0}

	rf.state = Follower
	rf.commitCh = make(chan struct{}, 10)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) doFollower() {
	// DPrintf("doFollower: server(%d), state(%d)", rf.me, rf.state)
	select {
	case <-electionTimeout(): // change to candidate
		rf.convertCandidate()
	case <-rf.recvLeader:
	}
}

func (rf *Raft) convertFollower() {
	defer rf.persist()
	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) doCandidate() {
	DPrintf("doCandidate: server(%d), state(%d)", rf.me, rf.state)

	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// send request vote for all server
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		reqVoteArgs := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm(),
		}
		rf.mu.Unlock()

		go func(server int) {

			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, reqVoteArgs, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// defer rf.persist()

			if reply.Term > rf.currentTerm {
				rf.convertFollower()
				rf.recvLeader <- struct{}{}
				return
			}
			if rf.state != Candidate {
				return
			}
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				rf.voteNum++
				// change to leader
				if rf.getMostVote() {
					rf.convertLeader()
					rf.recvLeader <- struct{}{}
					return
				}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.state = Candidate
	rf.votedFor = rf.me
	rf.voteNum = 1
	rf.currentTerm++
}

func (rf *Raft) doLeader() {
	DPrintf("doLeader: server(%d), state(%d), term(%d)", rf.me, rf.state, rf.currentTerm)
	rf.mu.Lock()
	// init nextIndex and matchIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	for {
		// check state
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		go func() {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				go func(server int) {
					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}

					entries := make([]*LogEntry, 0)
					nextIndex := rf.nextIndex[server]
					if len(rf.logs) >= nextIndex {
						entries = rf.logs[nextIndex:]
					}
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: nextIndex - 1,
						PrevLogTerm:  rf.logs[nextIndex-1].Term,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, args, reply)
					if !ok {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.convertFollower()
						return
					}

					if !reply.Success {
						rf.nextIndex[server]--
						return
					}

					rf.nextIndex[server] = rf.lastLogIndex() + 1
					rf.matchIndex[server] = rf.lastLogIndex()
					rf.checkCommitIndex()
				}(i)
			}
		}()
		// <-broadcastTime()
		time.Sleep(broadcastTime())
	}
}

func (rf *Raft) checkCommitIndex() {
	N := rf.lastLogIndex()
	for N > rf.commitIndex {
		matchNum := 1
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= N && rf.logs[N].Term == rf.currentTerm {
				matchNum++
				if matchNum >= len(rf.peers)/2+1 {
					rf.commitIndex = N
					rf.commitCh <- struct{}{}
					return
				}
			}
		}
		N--
	}
}

func (rf *Raft) convertLeader() {
	DPrintf("convertLeader server(%d)", rf.me)
	// defer rf.persist()
	rf.state = Leader
	// rf.votedFor = -1
	// rf.voteNum = 0
}

// heartbeat
func electionTimeout() <-chan time.Time {
	return time.After(time.Duration(500+rand.Int63()%500) * time.Millisecond)
}

// func broadcastTime() <-chan time.Time {
// 	return time.After(50 * time.Millisecond)
// }

func broadcastTime() time.Duration {
	return 50 * time.Millisecond
}

func (rf *Raft) lastLogIndex() int {
	logs := rf.logs
	return logs[len(logs)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	logs := rf.logs
	return logs[len(logs)-1].Term
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}
