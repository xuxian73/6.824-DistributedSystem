package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	currentLeader int
	clientId	int64
	requestId	int64
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
	ck.currentLeader = 0
	ck.clientId = nrand()
	ck.requestId = 1
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs {
		Key: key,
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	for  {
		reply := GetReply{}
		DPrintf("client %d send request %d %s Key %s", ck.clientId, args.RequestId, GetOp, args.Key)
		if (ck.servers[ck.currentLeader].Call("KVServer.Get", &args, &reply)) {
			if reply.Err == ErrNoKey || reply.Err == OK{
				ck.requestId += 1
				return reply.Value
			} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
				ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			} 
		} else {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		}
	}
	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs {
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	for  {
		reply := PutAppendReply{}
		DPrintf("client %d send request %d to %d %s Key %s, Value %s", ck.clientId, args.RequestId, ck.currentLeader, args.Op, args.Key, args.Value)
		if (ck.servers[ck.currentLeader].Call("KVServer.PutAppend", &args, &reply)) {
			if reply.Err == OK {
				ck.requestId += 1
				return
			} else {
				ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			} 
		} else {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
