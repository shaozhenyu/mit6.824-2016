package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	DPrintf("Get : |%s|", key)
	args := &GetArgs{
		Key: key,
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := &GetReply{}
			ok := ck.servers[i].Call("RaftKV.Get", args, reply)
			if !ok {
				continue
			}
			if reply.Err != "" || reply.WrongLeader {
				continue
			}
			return reply.Value
		}
	}

	return ""
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
	DPrintf("PutAppend: |%s| |%s| |%s|", key, value, op)
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := &PutAppendReply{}
			ok := ck.servers[i].Call("RaftKV.PutAppend", args, reply)
			if !ok {
				continue
			}
			if reply.Err != "" || reply.WrongLeader {
				continue
			}
			return
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
