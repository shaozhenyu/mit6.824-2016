package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
)

type Clerk struct {
	mu        sync.Mutex
	servers   []*labrpc.ClientEnd
	id        int64
	reqID     int64
	leaderIdx int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.reqID = 0
	ck.leaderIdx = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key, ClientID: ck.id}
	ck.mu.Lock()
	ck.reqID++
	args.ReqID = ck.reqID
	ck.mu.Unlock()

	// DPrintf("Get : clientID:|%d| req|%d| key|%s|", ck.id, args.ReqID, key)
	for {
		if ck.leaderIdx != -1 {
			ok, value := ck.get(args, ck.leaderIdx)
			if ok {
				return value
			}
		} else {
			for i := 0; i < len(ck.servers); i++ {
				ok, value := ck.get(args, i)
				if ok {
					return value
				}
			}
		}
	}
	return ""
}

func (ck *Clerk) get(args *GetArgs, server int) (ok bool, val string) {
	reply := &GetReply{}
	ok = ck.servers[server].Call("RaftKV.Get", args, reply)
	if reply.WrongLeader {
		ck.updateLeaderIdx(-1)
		ok = false
	} else {
		ck.updateLeaderIdx(server)
	}
	if reply.Err != "" {
		ok = false
	}
	val = reply.Value
	return
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.id,
	}
	ck.mu.Lock()
	ck.reqID++
	args.ReqID = ck.reqID
	ck.mu.Unlock()

	for {
		if ck.leaderIdx != -1 {
			ok := ck.putAppend(args, ck.leaderIdx)
			if ok {
				return
			}
		} else {
			for i := 0; i < len(ck.servers); i++ {
				ok := ck.putAppend(args, i)
				if ok {
					return
				}
			}
		}
	}
}

func (ck *Clerk) putAppend(args *PutAppendArgs, server int) (ok bool) {
	reply := &PutAppendReply{}
	ok = ck.servers[server].Call("RaftKV.PutAppend", args, reply)
	if reply.WrongLeader {
		ck.updateLeaderIdx(-1)
		ok = false
	} else {
		ck.updateLeaderIdx(server)
	}
	if reply.Err != "" {
		ok = false
	}
	return
}

func (ck *Clerk) updateLeaderIdx(idx int) {
	ck.mu.Lock()
	ck.leaderIdx = idx
	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
