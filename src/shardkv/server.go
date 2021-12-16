package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	dead 		 int32
	maxraftstate int // snapshot if log grows this big

	kvstore     [NShards]map[string]string
	notifier    map[int]chan Result
	lastRequest map[int64]int64
	mck			*shardctrler.Clerk
	config 		shardctrler.Config
	configNum	int
	// Your definitions here.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	result := kv.processOp(op)
	reply.Err, reply.Value = result.Err, result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	result := kv.processOp(op)
	reply.Err = result.Err
}

func (kv *ShardKV) makeNotifier(index int) chan Result {
	_, ok := kv.notifier[index]
	if ok {
		panic("Unexpected: notifier has existed")
	}
	kv.notifier[index] = make(chan Result)
	return kv.notifier[index]
}

func (kv *ShardKV) processOp(op Op) Result {
	DPrintf("server (%v,%v) start Op %v", kv.gid, kv.me, op)
	kv.mu.Lock()
	lastRequest, ok := kv.lastRequest[op.ClientId]
	if kv.config.Shards[key2shard(op.Key)] != kv.gid {
		DPrintf("server (%v,%v) reject %v for wrong group", kv.gid, kv.me, op)
		kv.mu.Unlock()
		return Result{ Err: ErrWrongGroup }
	}
	if ok && lastRequest >= op.RequestId && op.Type != "Get" {
		kv.mu.Unlock()
		return Result{Err: OK}
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("server (%v,%v) reject %v for not leader", kv.gid, kv.me, op)
		return Result{Err: ErrWrongLeader}
	}
	kv.mu.Lock()
	ch := kv.makeNotifier(index)
	kv.mu.Unlock()
	var result Result
	select {
	case result, ok = <-ch:
		if ok && result.ClientId == op.ClientId && result.RequestId == op.RequestId {

		} else {
			result.Err = ErrWrongLeader
		}
	case <- time.After(500 * time.Millisecond):
		result.Err = ErrTimeOut
	}
	DPrintf("server (%v,%v) reply with result %v", kv.gid, kv.me, result)
	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.notifier, index)
	}()
	return result
}


func (kv *ShardKV) handleGet(op Op) Result {
	value, ok := kv.kvstore[key2shard(op.Key)][op.Key]
	if ok {
		return Result{Err: OK, Value: value, ClientId: op.ClientId, RequestId: op.RequestId}
	} else {
		return Result{Err: ErrNoKey, Value: "", ClientId: op.ClientId, RequestId: op.RequestId}
	}
}

func (kv *ShardKV) handlePut(op Op) Result {
	kv.kvstore[key2shard(op.Key)][op.Key] = op.Value
	return Result{Err: OK, ClientId: op.ClientId, RequestId: op.RequestId}
}

func (kv *ShardKV) handleAppend(op Op) Result {
	value, ok := kv.kvstore[key2shard(op.Key)][op.Key]
	if !ok {
		kv.kvstore[key2shard(op.Key)][op.Key] = op.Value
	} else {
		kv.kvstore[key2shard(op.Key)][op.Key] = value + op.Value
	}
	return Result{Err: OK, ClientId: op.ClientId, RequestId: op.RequestId}
}
//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("server (%v, %v) is killed", kv.gid, kv.me)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) applier() {
	for !kv.killed(){
		msg := <-kv.applyCh

		if msg.CommandValid {
			kv.mu.Lock()
			DPrintf("server (%v,%v) apply a message, CommandValid: %t, CommandIndex: %d", kv.gid, kv.me, msg.CommandValid, msg.CommandIndex)
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
				DPrintf("server (%v,%v) install snapshot at index %d", kv.gid, kv.me, msg.CommandIndex)
				kv.installSnapshot(msg.CommandIndex)
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.readSnapshot(msg.Snapshot)
				DPrintf("server (%v,%v) get snapshot index %d", kv.gid, kv.me, msg.SnapshotIndex)
			}
			kv.mu.Unlock()
		} else {
			panic("unexpected msg")
		}
	}
}

func (kv *ShardKV) querier() {
	for !kv.killed(){
		config := kv.mck.Query(-1)
		DPrintf("server (%v, %v) query config %v, current %v", kv.gid,kv.me, config, kv.config)
		kv.mu.Lock()
		if config.Num > kv.config.Num {
			kv.configNum = config.Num
			kv.config = config
			DPrintf("server (%v,%v) update config %v, num: %v", kv.gid, kv.me, kv.config, kv.configNum)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) installSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastRequest)
	e.Encode(kv.kvstore)
	e.Encode(kv.config)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvstore [NShards]map[string]string
	var lastApplied map[int64]int64
	var config shardctrler.Config
	if d.Decode(&lastApplied) != nil ||
		d.Decode(&kvstore) != nil ||
		d.Decode(&config) != nil{
		DPrintf("decode error")
	} else {
		kv.lastRequest = lastApplied
		kv.kvstore = kvstore
		kv.config = config
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.dead = 0
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	for i := 0; i < NShards; i += 1 {
		kv.kvstore[i] = make(map[string]string)
	}
	kv.notifier = make(map[int]chan Result)
	kv.lastRequest = make(map[int64]int64)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = kv.mck.Query(0)
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	DPrintf("server (%v, %v) start config %v", kv.gid,kv.me, kv.config)
	go kv.applier()
	go kv.querier()
	return kv
}
