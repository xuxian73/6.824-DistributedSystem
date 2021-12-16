package shardctrler

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const CtrDebug = false

func CtrPrintf(format string, a ...interface{}) (n int, err error) {
	if CtrDebug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	notifier      map[int]chan Result
	lastRequest   map[int64]int64
	configs       []Config // indexed by config num
	currentConfig int      // config number
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:      Join,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	op.Servers = make(map[int][]string)
	for k := range args.Servers {
		op.Servers[k] = args.Servers[k]
	}
	result := sc.processOp(op)
	reply.Err = result.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:      Leave,
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	result := sc.processOp(op)
	reply.Err = result.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:      Move,
		GID:       args.GID,
		Shard:     args.Shard,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	result := sc.processOp(op)
	reply.Err = result.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:      Query,
		Num:       args.Num,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	result := sc.processOp(op)
	reply.Err, reply.Config = result.Err, result.Config
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	atomic.StoreInt32(&sc.dead, 1)
	CtrPrintf("server %v is killed", sc.me)
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) makeNotifier(index int) chan Result {
	_, ok := sc.notifier[index]
	if ok {
		panic("Unexpected: notifier has existed")
	}
	sc.notifier[index] = make(chan Result)
	return sc.notifier[index]
}

func (sc *ShardCtrler) processOp(op Op) Result {
	var result Result
	CtrPrintf("server %v process op %v", sc.me, op)
	sc.mu.Lock()
	lastRequest, ok := sc.lastRequest[op.ClientId]
	if ok && lastRequest >= op.RequestId && op.Type != Query {
		sc.mu.Unlock()
		return Result{Err: OK}
	}
	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		CtrPrintf("server %v reject %v for not leader", sc.me, op)
		return Result{Err: ErrWrongLeader}
	}
	sc.mu.Lock()
	ch := sc.makeNotifier(index)
	sc.mu.Unlock()
	select {
	case result, ok = <-ch:
		if ok && result.ClientId == op.ClientId && result.RequestId == op.RequestId {

		} else {
			result.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		result.Err = ErrTimeOut
	}
	CtrPrintf("server %v reply with result %v", sc.me, result)
	go func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		delete(sc.notifier, index)
	}()
	return result
}

func (sc *ShardCtrler) cloneConifg(index int) Config {
	config := Config{
		Num:    index,
		Shards: sc.configs[index].Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range sc.configs[index].Groups {
		servers := make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}
	return config
}

func (sc *ShardCtrler) findMaxShardGroup(distribution map[int][]int) int {
	_, ok := distribution[0]
	if ok && len(distribution[0]) > 0 {
		return 0
	}
	var keys []int
	var max, gid = 0, 0
	for k := range distribution {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		if max < len(distribution[k]) {
			max = len(distribution[k])
			gid = k
		}
	}
	return gid
}

func (sc *ShardCtrler) findMinShardGroup(distribution map[int][]int) int {
	if len(distribution) == 0 {
		return 0
	}
	var keys []int
	var min, gid = NShards + 1, 0
	for k := range distribution {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		if min > len(distribution[k]) && k != 0 {
			min = len(distribution[k])
			gid = k
		}
	}
	return gid
}

func (sc *ShardCtrler) handleJoin(op Op) Result {
	config := sc.cloneConifg(sc.currentConfig)

	distribution := make(map[int][]int)
	for gid := range op.Servers {
		config.Groups[gid] = op.Servers[gid]
		distribution[gid] = make([]int, 0)
	}
	for i := 0; i < NShards; i += 1 {
		distribution[config.Shards[i]] = append(distribution[config.Shards[i]], i)
	}
	for {
		src, dst := sc.findMaxShardGroup(distribution), sc.findMinShardGroup(distribution)
		if src != 0 && len(distribution[src])-len(distribution[dst]) <= 1 {
			break
		}
		config.Shards[distribution[src][0]] = dst
		distribution[dst] = append(distribution[dst], distribution[src][0])
		distribution[src] = distribution[src][1:]
	}
	config.Num += 1
	sc.currentConfig += 1
	sc.configs = append(sc.configs, config)
	return Result{Err: OK, ClientId: op.ClientId, RequestId: op.RequestId, Config: sc.configs[sc.currentConfig]}
}

func (sc *ShardCtrler) handleLeave(op Op) Result {
	config := sc.cloneConifg(sc.currentConfig)
	config.Num += 1
	sc.currentConfig += 1
	distribution := make(map[int][]int)

	for _, gid := range op.GIDs {
		delete(config.Groups, gid)
	}
	for gid := range config.Groups {
		distribution[gid] = make([]int, 0)
		for i := 0; i < NShards; i += 1 {
			if config.Shards[i] == gid {
				distribution[gid] = append(distribution[gid], i)
			}
		}
	}
	for _, src := range op.GIDs {
		for i := 0; i < NShards; i += 1 {
			if config.Shards[i] == src {
				dst := sc.findMinShardGroup(distribution)
				config.Shards[i] = dst
				if dst != 0 {
					distribution[dst] = append(distribution[dst], i)
				}
			}
		}
	}

	sc.configs = append(sc.configs, config)
	return Result{Err: OK, ClientId: op.ClientId, RequestId: op.RequestId, Config: sc.configs[sc.currentConfig]}
}

func (sc *ShardCtrler) handleMove(op Op) Result {
	config := sc.cloneConifg(sc.currentConfig)
	config.Num += 1
	sc.currentConfig += 1
	config.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, config)
	return Result{Err: OK, ClientId: op.ClientId, RequestId: op.RequestId, Config: sc.configs[sc.currentConfig]}
}

func (sc *ShardCtrler) handleQuery(op Op) Result {
	if op.Num == -1 || op.Num > sc.currentConfig {
		return Result{Err: OK, ClientId: op.ClientId, RequestId: op.RequestId, Config: sc.configs[sc.currentConfig]}
	} else {
		return Result{Err: OK, ClientId: op.ClientId, RequestId: op.RequestId, Config: sc.configs[op.Num]}
	}
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh
		CtrPrintf("server %v apply msg %v", sc.me, msg)
		sc.mu.Lock()
		op := msg.Command.(Op)
		var result Result
		lastRequest, ok := sc.lastRequest[op.ClientId]
		if !ok {
			lastRequest, sc.lastRequest[op.ClientId] = 0, 0
		}
		if lastRequest < op.RequestId || op.Type == Query {
			switch op.Type {
			case Join:
				result = sc.handleJoin(op)
			case Leave:
				result = sc.handleLeave(op)
			case Move:
				result = sc.handleMove(op)
			case Query:
				result = sc.handleQuery(op)
			}
			sc.lastRequest[op.ClientId] = op.RequestId
		} else {
			result.Err, result.ClientId, result.RequestId = OK, op.ClientId, op.RequestId
		}

		notifier, ok := sc.notifier[msg.CommandIndex]
		if ok {
			notifier <- result
		}
		sc.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.dead = 0
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.notifier = make(map[int]chan Result)
	sc.lastRequest = make(map[int64]int64)
	sc.currentConfig = 0

	go sc.applier()
	return sc
}
