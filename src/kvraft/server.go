package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	kvstore      map[string]string
	notifier     map[int]chan Result
	lastRequest  map[int64]int64 // key: clientId, Value: requestId this server last apply for the client
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	DPrintf("%d start op %s Key: %s", kv.me, op.Type, op.Key)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("%d is not leader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.makeNotifier(index)
	kv.mu.Unlock()
	timeout_timer := time.NewTimer(500 * time.Millisecond)
	select {
	case result, ok := <-ch:
		// if the channel is closed, that means the commit op
		// at the index does not match this one. Thus the leadership
		// has changed
		DPrintf("%d %d %s %s", args.ClientId, args.RequestId, result.Err, result.Value)

		if ok && result.ClientId == args.ClientId && result.RequestId == args.RequestId {
			reply.Err, reply.Value = result.Err, result.Value
			DPrintf("%d reply Clinet %d Request %d %s op Key %s Value %s, reply Err: %s, Value: %s",
				kv.me, args.ClientId, args.RequestId, op.Type, op.Key, op.Value, reply.Err, reply.Value)
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- timeout_timer.C:
		// if do not set timeout timer the cleck would stuck
		// if the current server is leader but is partitioned
		// since the user call is sync
		reply.Err = ErrTimeOut
	}

	go func(){ 
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.notifier, index)
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	DPrintf("%d start op %s Key: %s, Value: %s", kv.me, op.Type, op.Key, op.Value)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("%d reject op %s Key: %s, Value: %s for not leader", kv.me, op.Type, op.Key, op.Value)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.makeNotifier(index)
	kv.mu.Unlock()
	timeout_timer := time.NewTimer(500 * time.Millisecond)
	select {
	case result, ok := <-ch:
		// if the channel is closed, that means the commit op
		// at the index does not match this one. Thus the leadership
		// has changed
		DPrintf("%d %d %s %s", args.ClientId, args.RequestId, result.Err, result.Value)

		if ok && result.ClientId == args.ClientId && result.RequestId == args.RequestId {
			reply.Err = result.Err
		} else {
			reply.Err = ErrWrongLeader
		}
		DPrintf("%d reply Clinet %d Request %d %s op Key %s Value %s, reply Err: %s",
				kv.me, args.ClientId, args.RequestId, op.Type, op.Key, op.Value, reply.Err)
	case <-timeout_timer.C:
		reply.Err = ErrTimeOut
	}
	go func(){ 
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.notifier, index)
	}()
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			kv.mu.Lock()
			DPrintf("server %d apply a message, CommandValid: %t, CommandIndex: %d", kv.me, msg.CommandValid, msg.CommandIndex)
			op := msg.Command.(Op)
			var result Result
			lastRequest, ok := kv.lastRequest[op.ClientId]
			if !ok {
				lastRequest, kv.lastRequest[op.ClientId] = 0, 0
			}
			if lastRequest < op.RequestId || op.Type == GetOp {
				switch op.Type {
				case GetOp:
					result = kv.handleGet(op)
				case PutOp:
					result = kv.handlePut(op)
				case AppendOp:
					result = kv.handleAppend(op)
				}
				kv.lastRequest[op.ClientId] = op.RequestId
			} else {
				result.Err, result.ClientId, result.RequestId = OK, op.ClientId, op.RequestId
			}
			notifier, ok := kv.notifier[msg.CommandIndex]
			if ok {
				notifier <- result
			}
			if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate {
				DPrintf("%d install snapshot at index %d", kv.me, msg.CommandIndex)
				kv.installSnapshot(msg.CommandIndex)
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.readSnapshot(msg.Snapshot)
				DPrintf("%d get snapshot index %d", kv.me, msg.SnapshotIndex)
			}
			kv.mu.Unlock()
		} else {
			panic("unexpected msg")
		}
	}
}

func (kv *KVServer) handleGet(op Op) Result {
	value, ok := kv.kvstore[op.Key]
	DPrintf("%d server handle ApplyMsg %s Key %s, Value %s", kv.me, op.Type, op.Key, value)
	if ok {
		return Result{Err: OK, Value: value, ClientId: op.ClientId, RequestId: op.RequestId}
	} else {
		return Result{Err: ErrNoKey, Value: "", ClientId: op.ClientId, RequestId: op.RequestId}
	}
}

func (kv *KVServer) handlePut(op Op) Result {
	DPrintf("%d server handle ApplyMsg %s Key %s, Value %s", kv.me, op.Type, op.Key, op.Value)
	kv.kvstore[op.Key] = op.Value
	return Result{Err: OK, ClientId: op.ClientId, RequestId: op.RequestId}
}

func (kv *KVServer) handleAppend(op Op) Result {
	value, ok := kv.kvstore[op.Key]
	if !ok {
		kv.kvstore[op.Key] = op.Value
	} else {
		kv.kvstore[op.Key] = value + op.Value
	}
	DPrintf("%d server handle ApplyMsg %s Key %s, Value %s", kv.me, op.Type, op.Key, kv.kvstore[op.Key])
	return Result{Err: OK, ClientId: op.ClientId, RequestId: op.RequestId}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("%d server is killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) makeNotifier(index int) chan Result {
	_, ok := kv.notifier[index]
	if ok {
		panic("Unexpected: notifier has existed")
	}
	kv.notifier[index] = make(chan Result)
	return kv.notifier[index]
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvstore map[string]string
	var lastApplied map[int64]int64
	if d.Decode(&lastApplied) != nil ||
		d.Decode(&kvstore) != nil {
		DPrintf("decode error")
	} else {
		kv.lastRequest = lastApplied
		kv.kvstore = kvstore
	}
}

func (kv *KVServer) installSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastRequest)
	e.Encode(kv.kvstore)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvstore = make(map[string]string)
	kv.notifier = make(map[int]chan Result)
	kv.lastRequest = make(map[int64]int64)
	kv.readSnapshot(persister.ReadSnapshot())
	DPrintf("%d start, maxraftstate: %d", kv.me, kv.maxraftstate)
	// You may need initialization code here.
	go kv.applier()
	return kv
}
