package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key         string
	Value       string
	OperateType string
	ReqID       int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	reqID     int64
	applyChan map[int64]chan Op
	data      map[string]string
}

func newOp(key, value string, opType string, reqID int64) Op {
	return Op{
		Key:         key,
		Value:       value,
		OperateType: opType,
		ReqID:       reqID,
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	_, ok := kv.rf.GetState()
	if !ok {
		reply.WrongLeader = true
	} else {
		kv.mu.Lock()
		kv.reqID++
		reqID := kv.reqID
		kv.applyChan[reqID] = make(chan Op, 1)
		rsp := kv.applyChan[reqID]
		kv.mu.Unlock()

		op := newOp(args.Key, "", "Get", reqID)
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		var rspOp Op
		select {
		case rspOp = <-rsp:
			reply.Value = rspOp.Value
		case <-time.After(1 * time.Second):
			DPrintf("get timeout")
			reply.Err = "request timeout"
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, ok := kv.rf.GetState()
	if !ok {
		reply.WrongLeader = true
	} else {
		kv.mu.Lock()
		kv.reqID++
		reqID := kv.reqID
		kv.applyChan[reqID] = make(chan Op, 1)
		rsp := kv.applyChan[reqID]
		kv.mu.Unlock()

		op := newOp(args.Key, args.Value, args.Op, reqID)
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		select {
		case <-rsp:
		case <-time.After(1 * time.Second):
			DPrintf("PutAppend timeout")
			reply.Err = "request timeout"
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.applyChan = make(map[int64]chan Op)
	kv.data = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.recvApply()

	return kv
}

func (kv *RaftKV) recvApply() {
	for applyMsg := range kv.applyCh {
		DPrintf("recvApply %v %v", applyMsg, applyMsg.Command)
		go kv.handleAppledCommand(applyMsg)
	}
}

func (kv *RaftKV) handleAppledCommand(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op, _ := msg.Command.(Op)
	DPrintf("op:%v, data:%v", op, kv.data)
	switch op.OperateType {
	case "Get":
		op.Value = kv.data[op.Key]
	case "Put":
		kv.data[op.Key] = op.Value
	case "Append":
		kv.data[op.Key] += op.Value
	}
	kv.applyChan[op.ReqID] <- op
}
