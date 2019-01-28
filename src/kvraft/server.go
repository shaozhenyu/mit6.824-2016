package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

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

	// check client req
	ClientID  int64
	ClientReq int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	reqID     int64
	applyChan map[int]chan Op
	data      map[string]string

	// applyed client reqest
	applyCliReq map[int64]map[int64]struct{}
}

func newOp(key, value string, opType string, reqID, clientID, clientReq int64) Op {
	return Op{
		Key:         key,
		Value:       value,
		OperateType: opType,
		ReqID:       reqID,
		ClientID:    clientID,
		ClientReq:   clientReq,
	}
}

func checkRspConsistent(req, rsp Op) bool {
	if req.ClientID != rsp.ClientID || req.ClientReq != rsp.ClientReq {
		return false
	}
	return true
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	_, ok := kv.rf.GetState()
	if !ok {
		reply.WrongLeader = true
	} else {
		op := newOp(args.Key, "", "Get", 0, args.ClientID, args.ReqID)
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		kv.mu.Lock()
		if _, ok := kv.applyChan[index]; !ok {
			kv.applyChan[index] = make(chan Op, 1)
		}
		rsp := kv.applyChan[index]
		kv.mu.Unlock()

		var rspOp Op
		select {
		case rspOp = <-rsp:
			if !checkRspConsistent(op, rspOp) {
				reply.Err = "request error"
			}
			reply.Value = rspOp.Value
		case <-time.After(1 * time.Second):
			reply.Err = "request timeout"
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, ok := kv.rf.GetState()
	if !ok {
		reply.WrongLeader = true
	} else {
		op := newOp(args.Key, args.Value, args.Op, 0, args.ClientID, args.ReqID)
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}

		kv.mu.Lock()
		if _, ok := kv.applyChan[index]; !ok {
			kv.applyChan[index] = make(chan Op, 1)
		}
		rsp := kv.applyChan[index]
		kv.mu.Unlock()

		var rspOp Op
		select {
		case rspOp = <-rsp:
			if !checkRspConsistent(op, rspOp) {
				reply.Err = "request error"
			}
		case <-time.After(1 * time.Second):
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
	kv.applyChan = make(map[int]chan Op, 100)
	kv.data = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.applyCliReq = make(map[int64]map[int64]struct{})

	go kv.recvApply()

	// if kv.maxraftstate != -1 {
	// 	go kv.doSnapshot()
	// }

	return kv
}

func (kv *RaftKV) recvApply() {
	for applyMsg := range kv.applyCh {
		kv.handleAppledCommand(applyMsg)
	}
}

func (kv *RaftKV) handleAppledCommand(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("server(%v) data(%v)", kv.me, kv.data)

	if msg.UseSnapshot {
		DPrintf("server(%d)read snapshot", kv.me)
		kv.readSnapshot(msg.Snapshot)
		return
	}

	index := msg.Index
	op, _ := msg.Command.(Op)

	DPrintf("index == (%d)", index)
	defer func() {
		kv.sendSnapshot(index)
		if _, ok := kv.applyChan[index]; ok {
			select {
			case <-kv.applyChan[index]:
			default:
			}
			kv.applyChan[index] <- op
		}
	}()

	if _, ok := kv.applyCliReq[op.ClientID]; ok {
		if _, ok := kv.applyCliReq[op.ClientID][op.ClientReq]; ok {
			if op.OperateType == "Get" {
				op.Value = kv.data[op.Key]
			}
			return
		}
	}
	// DPrintf("%d -- op:%v, data:%v", kv.me, op, kv.data)
	switch op.OperateType {
	case "Get":
		op.Value = kv.data[op.Key]
	case "Put":
		kv.data[op.Key] = op.Value
	case "Append":
		kv.data[op.Key] += op.Value
	}

	if _, ok := kv.applyCliReq[op.ClientID]; !ok {
		kv.applyCliReq[op.ClientID] = make(map[int64]struct{})
	}
	kv.applyCliReq[op.ClientID][op.ClientReq] = struct{}{}
}

func (kv *RaftKV) sendSnapshot(index int) {
	if kv.maxraftstate == -1 || kv.rf.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.applyCliReq)
	kv.rf.DoSnapshot(w.Bytes(), index)
}

func (kv *RaftKV) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	if kv.data == nil {
		kv.data = make(map[string]string)
	}
	d.Decode(&kv.data)
	if kv.applyCliReq == nil {
		kv.applyCliReq = make(map[int64]map[int64]struct{})
	}
	d.Decode(&kv.applyCliReq)
}
