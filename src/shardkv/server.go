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
	dead         int32
	maxraftstate int // snapshot if log grows this big

	shards        map[int]*Shard
	notifier      map[int]chan Result
	lastRequest   map[int64]int64
	mck           *shardctrler.Clerk
	currentConfig shardctrler.Config
	lastConfig    shardctrler.Config
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
	result := kv.processCommand(NewOperation(&op))
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
	result := kv.processCommand(NewOperation(&op))
	reply.Err = result.Err
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err, reply.Num = ErrUnmatchConfig, kv.currentConfig.Num
		DPrintf("server (%v, %v) reject pullshard request for not ready", kv.gid, kv.me)
		return
	}
	reply.ShardData = make(map[int]map[string]string)
	reply.LastRequest = make(map[int64]int64)
	for _, shard := range args.ShardId {
		reply.ShardData[shard] = CopyShardData(kv.shards[shard].Storage)
	}
	for clientID, requestId := range kv.lastRequest {
		reply.LastRequest[clientID] = requestId
	}
	reply.Err, reply.Num = OK, kv.currentConfig.Num
	DPrintf("server (%v, %v) reply pull shard request, ConfigNum: %v, ShardData: %v, LastRequest: %v", kv.gid, kv.me, reply.Num, reply.ShardData, reply.LastRequest)
}

func (kv *ShardKV) Acknowledge(args *AcknowledgeArgs, reply *AcknowledgeReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err, reply.Num = OK, kv.currentConfig.Num
		DPrintf("server (%v, %v) reply duplicated Acknowledge Arg Num:%v, reply Num: %v", kv.gid, kv.me, args.ConfigNum, reply.Num)
		return
	}
	
	kv.mu.Unlock()
	result := kv.processCommand(NewAcknowledge(args))
	reply.Err = result.Err
	DPrintf("server (%v, %v) reply acknowledge Err: %v", kv.gid, kv.me, reply.Err)
}

func (kv *ShardKV) makeNotifier(index int) chan Result {
	_, ok := kv.notifier[index]
	if ok {
		panic("Unexpected: notifier has existed")
	}
	kv.notifier[index] = make(chan Result)
	return kv.notifier[index]
}

func (kv *ShardKV) processCommand(command Command) Result {
	// if command.Type == Operation {
	// 	kv.mu.Lock()
	// 	op := command.Data.(Op)
	// 	lastRequest, ok := kv.lastRequest[op.ClientId]
	// 	if kv.currentConfig.Shards[key2shard(op.Key)] != kv.gid {
	// 		DPrintf("server (%v,%v) reject %v for wrong group", kv.gid, kv.me, op)
	// 		kv.mu.Unlock()
	// 		return Result{Err: ErrWrongGroup}
	// 	}
	// 	if ok && lastRequest >= op.RequestId && op.Type != "Get" {
	// 		kv.mu.Unlock()
	// 		return Result{Err: OK}
	// 	}
	// 	kv.mu.Unlock()
	// }
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return Result{Err: ErrWrongLeader}
	}
	DPrintf("server (%v,%v) start Command %v", kv.gid, kv.me, command)
	kv.mu.Lock()
	ch := kv.makeNotifier(index)
	kv.mu.Unlock()
	var result Result
	select {
	case result = <-ch:
	case <-time.After(500 * time.Millisecond):
		result.Err = ErrTimeOut
	}
	DPrintf("server (%v,%v) process Command: %v with result %v", kv.gid, kv.me, command, result)
	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.notifier, index)
	}()
	return result
}

func (kv *ShardKV) isServe(key string) bool {
	shard := key2shard(key)
	return kv.currentConfig.Shards[shard] == kv.gid && (kv.shards[shard].State == Serving || kv.shards[shard].State == Acknowledging)
}

func (kv *ShardKV) handleOperation(op *Op) Result {
	if !kv.isServe(op.Key) {
		return Result{Err: ErrWrongGroup}
	}
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
		result.Err = OK
	}
	return result
}
func (kv *ShardKV) handleGet(op *Op) Result {
	value, ok := kv.shards[key2shard(op.Key)].Storage[op.Key]
	if ok {
		return Result{Err: OK, Value: value}
	} else {
		return Result{Err: ErrNoKey, Value: ""}
	}
}

func (kv *ShardKV) handlePut(op *Op) Result {
	kv.shards[key2shard(op.Key)].Storage[op.Key] = op.Value
	return Result{Err: OK}
}

func (kv *ShardKV) handleAppend(op *Op) Result {
	value, ok := kv.shards[key2shard(op.Key)].Storage[op.Key]
	if !ok {
		kv.shards[key2shard(op.Key)].Storage[op.Key] = op.Value
	} else {
		kv.shards[key2shard(op.Key)].Storage[op.Key] = value + op.Value
	}
	return Result{Err: OK}
}

func (kv *ShardKV) updateShardState(config *shardctrler.Config) {
	for i := 0; i < shardctrler.NShards; i += 1 {
		if kv.currentConfig.Shards[i] == kv.gid && config.Shards[i] != kv.gid {
			if config.Shards[i] != 0 {
				kv.shards[i].State = BePulling
			}
		} else if kv.currentConfig.Shards[i] != kv.gid && config.Shards[i] == kv.gid {
			if kv.currentConfig.Shards[i] != 0 {
				kv.shards[i].State = Pulling
			}
		}
	}
}

func (kv *ShardKV) handleConfiguration(NewConfig *shardctrler.Config) Result {
	if NewConfig.Num == kv.currentConfig.Num+1 {
		kv.updateShardState(NewConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *NewConfig
		DPrintf("server (%v, %v) update configuration: %v, ShardState: %v", kv.gid, kv.me, kv.currentConfig, kv.ShardState())
	}
	return Result{Err: OK}
}

func (kv *ShardKV) handleInstallShard(reply *PullShardReply) Result {
	var result Result
	if reply.Num == kv.currentConfig.Num {
		DPrintf("server (%v, %v) handle InstallShard, ShardData: %v, LastRequest: %v, before shard: %v", kv.gid, kv.me, reply.ShardData, reply.LastRequest, kv.shards)
		for id, data := range reply.ShardData {
			shard := kv.shards[id]
			if shard.State == Pulling {
				for k, v := range data {
					shard.Storage[k] = v
				}
				shard.State = Acknowledging
			}
		}
		for clientId, requestId := range reply.LastRequest {
			if kv.lastRequest[clientId] < requestId {
				kv.lastRequest[clientId] = requestId
			}
		}
		result.Err = OK
		DPrintf("server (%v, %v) handle InstallShard, ShardData: %v, LastRequest: %v, after shard: %v", kv.gid, kv.me, reply.ShardData, reply.LastRequest, kv.shards)
	} else {
		DPrintf("server (%v, %v) handle Acknowledge, before shard: %v", kv.gid, kv.me, kv.shards)
		result.Err = ErrUnmatchConfig
	}
	return result
}

func (kv *ShardKV) handleAcknowledge(args *AcknowledgeArgs) Result {
	var result Result
	if args.ConfigNum == kv.currentConfig.Num {
		DPrintf("server (%v, %v) handle Acknowledge, before status %v", kv.gid, kv.me, kv.ShardState())
		for _, shard := range args.ShardId {
			switch kv.shards[shard].State {
			case BePulling:
				kv.shards[shard] = NewShard()
			case Acknowledging:
				kv.shards[shard].State = Serving
			}
		}
		result.Err = OK
		DPrintf("server (%v, %v) handle Acknowledge, after status %v", kv.gid, kv.me, kv.ShardState())
	} else {
		DPrintf("server (%v, %v) handle duplicated acknowledge Num: %v, Shards: %v, current config Num: %v", kv.gid, kv.me, args.ConfigNum, args.ShardId, kv.currentConfig.Num)
		result.Err = OK
	}
	return result
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

func (kv *ShardKV) ShardState() []ShardState {
	var ret []ShardState
	for i := 0; i < shardctrler.NShards; i += 1 {
		ret = append(ret, kv.shards[i].State)
	}
	return ret
}
func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			kv.mu.Lock()
			var result Result
			command := msg.Command.(Command)
			DPrintf("server (%v,%v) apply a message, CommandValid: %t, CommandIndex: %d, CommandType: %v", kv.gid, kv.me, msg.CommandValid, msg.CommandIndex, command.Type)
			switch command.Type {
			case Operation:
				op := command.Data.(Op)
				result = kv.handleOperation(&op)
			case Configuration:
				newConfig := command.Data.(shardctrler.Config)
				result = kv.handleConfiguration(&newConfig)
			case InstallShard:
				reply := command.Data.(PullShardReply)
				result = kv.handleInstallShard(&reply)
			case Acknowledge:
				args := command.Data.(AcknowledgeArgs)
				result = kv.handleAcknowledge(&args)
			}
			_, isleader := kv.rf.GetState()
			notifier, ok := kv.notifier[msg.CommandIndex]
			if ok && isleader {
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

func (kv *ShardKV) isOkToConfig() bool {
	for _, shard := range kv.shards {
		if shard.State != Serving {
			return false
		}
	}
	return true
}

func (kv *ShardKV) querier() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			ok := kv.isOkToConfig()
			currentConifgNum := kv.currentConfig.Num
			kv.mu.Unlock()
			if ok {
				NewConfig := kv.mck.Query(currentConifgNum + 1)
				if NewConfig.Num == currentConifgNum+1 {
					kv.processCommand(NewConfiguration(&NewConfig))
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) make_group_end(gid int) []*labrpc.ClientEnd {
	ret := make([]*labrpc.ClientEnd, 0)
	for _, server := range kv.lastConfig.Groups[gid] {
		ret = append(ret, kv.make_end(server))
	}
	return ret
}

func (kv *ShardKV) Gid2ShardsOfState(state ShardState) map[int][]int {
	gid2shards := make(map[int][]int) // k: gid, v: shards to pull
	for i := 0; i < shardctrler.NShards; i += 1 {
		if kv.shards[i].State == state {
			gid2shards[kv.lastConfig.Shards[i]] = append(gid2shards[kv.lastConfig.Shards[i]], i)
		}
	}
	return gid2shards
}

func (kv *ShardKV) puller() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			gid2shards := kv.Gid2ShardsOfState(Pulling)
			configNum := kv.currentConfig.Num
			if len(gid2shards) > 0 {
				DPrintf("server(%v, %v) start pull shard task, GID-Shards: %v", kv.gid, kv.me, gid2shards)
			}
			Servers := make(map[int][]*labrpc.ClientEnd)
			for gid := range gid2shards {
				Servers[gid] = kv.make_group_end(gid)
			}
			kv.mu.Unlock()
			var wg sync.WaitGroup
			for gid, shardid := range gid2shards {
				wg.Add(1)
				Server := Servers[gid]
				args := PullShardArgs{ConfigNum: configNum, ShardId: shardid}
				go func(Server []*labrpc.ClientEnd, args PullShardArgs) {
					defer wg.Done()
					for _, srv := range Server {
						var reply PullShardReply
						if ok := srv.Call("ShardKV.PullShard", &args, &reply); ok && reply.Err == OK {
							kv.processCommand(NewInstallShard(&reply))
						}
					}
				}(Server, args)
			}
			wg.Wait()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) acknowledger() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			gid2shards := kv.Gid2ShardsOfState(Acknowledging)
			configNum := kv.currentConfig.Num
			if len(gid2shards) > 0 {
				DPrintf("server(%v, %v) start acknowledge task, GID-Shards: %v", kv.gid, kv.me, gid2shards)
			}
			Servers := make(map[int][]*labrpc.ClientEnd)
			for gid := range gid2shards {
				Servers[gid] = kv.make_group_end(gid)
			}
			kv.mu.Unlock()
			var wg sync.WaitGroup
			for gid, shardid := range gid2shards {
				wg.Add(1)
				Server := Servers[gid]
				args := AcknowledgeArgs{ConfigNum: configNum, ShardId: shardid}
				go func(Server []*labrpc.ClientEnd, args AcknowledgeArgs) {
					defer wg.Done()
					for _, srv := range Server {
						var reply AcknowledgeReply
						if ok := srv.Call("ShardKV.Acknowledge", &args, &reply); ok && reply.Err == OK {
							kv.processCommand(NewAcknowledge(&args))
						}
					}
				}(Server, args)
			}
			wg.Wait()
		}
	}
	time.Sleep(50 * time.Millisecond)
}

func (kv *ShardKV) installSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastRequest)
	e.Encode(kv.shards)
	e.Encode(kv.currentConfig)
	e.Encode(kv.lastConfig)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		for i := 0; i < shardctrler.NShards; i += 1 {
			kv.shards[i] = NewShard()
		}
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvstore map[int]*Shard
	var lastApplied map[int64]int64
	var currentConfig shardctrler.Config
	var lastConfig shardctrler.Config
	if d.Decode(&lastApplied) != nil ||
		d.Decode(&kvstore) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&lastConfig) != nil {
		DPrintf("decode error")
	} else {
		kv.lastRequest = lastApplied
		kv.shards = kvstore
		kv.currentConfig = currentConfig
		kv.lastConfig = lastConfig
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
	labgob.Register(Command{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullShardReply{})
	labgob.Register(AcknowledgeArgs{})

	kv := new(ShardKV)
	kv.dead = 0
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.shards = make(map[int]*Shard)
	kv.notifier = make(map[int]chan Result)
	kv.lastRequest = make(map[int64]int64)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.currentConfig = shardctrler.NewConfig()
	kv.lastConfig = shardctrler.NewConfig()
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	DPrintf("server (%v, %v) start config %v", kv.gid, kv.me, kv.currentConfig)
	go kv.applier()
	go kv.querier()
	go kv.puller()
	go kv.acknowledger()
	return kv
}
