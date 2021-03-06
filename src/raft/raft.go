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
	commitCh   chan struct{}
}

type logEntries struct {
	Index   int
	Term    int
	Command interface{}
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
	LastLogIndex int
	LastLogTerm  int
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
		rf.convertFollower()
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
	if args.LastLogTerm > rf.lastLogTerm() {
		return true
	}
	if args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex() {
		return true
	}
	// fmt.Printf("rf(%d) last term(%d), index(%d), args(%d) last term(%d), index(%d)\n",
	// 	rf.me, rf.lastLogTerm(), rf.lastLogIndex(), args.CandidateId, args.LastLogTerm, args.LastLogIndex)
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// append log rpc
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertFollower()
	}

	// 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false
	if len(rf.logs) > args.PrevLogIndex && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		for i := 1; i < len(rf.logs); i++ {
			if rf.logs[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if len(rf.logs) <= args.PrevLogIndex {
		reply.ConflictTerm = 0
		reply.ConflictIndex = len(rf.logs)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的
	// 附加任何在已有的日志中不存在的条目
	if len(args.Entries) > 0 {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
	}

	// 如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
	if args.LeaderCommit > rf.commitIndex {
		if len(args.Entries) == 0 || args.LeaderCommit < rf.logs[rf.lastLogIndex()].Index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.logs[rf.lastLogIndex()].Index
		}
		rf.commitCh <- struct{}{}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.lastLogIndex()
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if rf.state == Leader {
		rf.logs = append(rf.logs, &logEntries{
			Term:    rf.currentTerm,
			Index:   rf.lastLogIndex() + 1,
			Command: command,
		})
		index = rf.lastLogIndex()
		term = rf.currentTerm
	}
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
	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) convertCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votedNum = 1
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
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm(),
		}
		rf.mu.Unlock()

		go func(server int, args RequestVoteArgs) {
			requestVoteReply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, requestVoteReply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 已经产生leader
				if requestVoteReply.Term > rf.currentTerm {
					rf.convertFollower()
					rf.recvLeader <- struct{}{}
					return
				}
				// 获取选票失败
				if requestVoteReply.Term < rf.currentTerm || requestVoteReply.VoteGranted == false || rf.state != Candidate {
					return
				}
				// 获取选票成功
				if requestVoteReply.Term == rf.currentTerm && requestVoteReply.VoteGranted == true {
					rf.votedNum += 1
					if rf.getMostServiceVote() {
						rf.convertLeader()
						rf.recvLeader <- struct{}{}
						return
					}
				}
			}
		}(i, requestVoteArgs)
	}

	select {
	case <-time.After(candidateTimeout()):
	case <-rf.recvLeader:
	}
}

func (rf *Raft) convertLeader() {
	rf.state = Leader
	rf.votedFor = -1

	// 重新初始化nextIndex和matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) handleLeader() {
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
				go func(server int) {
					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					// 准备存储的日志条目
					entries := make([]*logEntries, 0)
					nextIndex := rf.nextIndex[server]
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
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, appendEntriesArgs, reply)
					if ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()

						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.convertFollower()
							return
						}
						// 状态判断
						if rf.state != Leader || rf.currentTerm != appendEntriesArgs.Term {
							return
						}
						if reply.Success {
							rf.matchIndex[server] = rf.lastLogIndex()
							rf.nextIndex[server] = rf.lastLogIndex() + 1
							rf.addCommitIndex()
						} else {
							rf.nextIndex[server] = rf.newAppendNextIndex(reply)
						}
					}
				}(i)
			}
		}()
		time.Sleep(heartBeatTime())
	}
}

// 使跟随者的日志进入和自己一致的状态
func (rf *Raft) newAppendNextIndex(reply *AppendEntriesReply) int {
	newIndex := reply.ConflictIndex
	for j := 1; j < len(rf.logs); j++ {
		entry := rf.logs[j]
		if entry.Term == reply.ConflictTerm {
			newIndex = j + 1
		}
	}
	if newIndex == 0 {
		newIndex = 1
	}
	return newIndex
}

// 如果存在一个满足N > commitIndex的 N，并且大多数的matchIndex[i] ≥ N成立，
// 并且log[N].term == currentTerm成立,那么令 commitIndex 等于这个 N
func (rf *Raft) addCommitIndex() {
	N := rf.lastLogIndex()
	for N > rf.commitIndex {
		matchNums := 1
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= N {
				matchNums++
			}
			if matchNums >= len(rf.peers)/2+1 && rf.logs[N].Term == rf.currentTerm {
				rf.commitIndex = N
				rf.commitCh <- struct{}{}
				return
			}
		}
		N--
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

// 持久化日志
func (rf *Raft) persistLogs(applyCh chan ApplyMsg) {
	for range rf.commitCh {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			log := rf.logs[rf.lastApplied]
			applyCh <- ApplyMsg{
				Index:   log.Index,
				Command: log.Command,
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

	// Your initialization code here.
	rf.state = Candidate
	rf.logs = make([]*logEntries, 1)
	rf.logs[0] = &logEntries{0, 0, nil}
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.recvLeader = make(chan struct{}, 10)
	rf.commitCh = make(chan struct{}, 10)
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// run
	go rf.startWork()
	// persist logs
	go rf.persistLogs(applyCh)

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
