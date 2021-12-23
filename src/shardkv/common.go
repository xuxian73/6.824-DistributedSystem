package shardkv

import (
	"fmt"
	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const NShards = 10

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
	ConfigOp = "Config"
)

type Err int8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrWrongGroup
	ErrTimeOut
	ErrUnmatchConfig
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrWrongGroup:
		return "ErrWrongGroup"
	case ErrTimeOut:
		return "ErrTimeOut"
	case ErrUnmatchConfig:
		return "ErrMatchConfig"
	default:
		return "UnknownErr"
	}
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

func (op Op) String() string {
	switch op.Type {
	case "Get":
		return fmt.Sprintf("%v Key: %v, ClientId: %v, RequestId: %v", op.Type, op.Key, op.ClientId, op.RequestId)
	case "Put":
		return fmt.Sprintf("%v Key: %v, Value: %v, ClientId: %v, RequestId: %v", op.Type, op.Key, op.Value, op.ClientId, op.RequestId)
	case "Append":
		return fmt.Sprintf("%v Key: %v, Value: %v, ClientId: %v, RequestId: %v", op.Type, op.Key, op.Value, op.ClientId, op.RequestId)
	default:
		return "Unknown Operation"
	}
}

type CommandType int8

const (
	Operation CommandType = iota
	Configuration
	InstallShard
	Acknowledge
	EmptyCommand
)

func (commandType CommandType) String() string {
	switch commandType {
	case Operation: return "Operation"
	case Configuration: return "Configuration"
	case InstallShard: return "InstallShard"
	case Acknowledge: return "Acknowledge"
	case EmptyCommand: return "EmptyCommand"
	default: return "Unknown Command"
	}
}
type Command struct {
	Type CommandType
	Data interface{}
}

func (command Command) String() string {
	return fmt.Sprintf("Command: %v, Data: {%v}", command.Type, command.Data)
}

type ShardState int8

const (
	Serving ShardState = iota
	Pulling 
	BePulling
	Acknowledging
)

func (state ShardState) String() string {
	switch state {
	case Serving: return "Serving"
	case Pulling: return "Pulling"
	case BePulling: return "BePulling"
	case Acknowledging: return "Acknowledging"
	default: return "Unknown State"
	}
}

func NewOperation(op *Op) Command {
	return Command{Operation, *op}
}

func NewConfiguration(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

func NewInstallShard(PullShardReply *PullShardReply) Command {
	return Command{InstallShard, *PullShardReply}
}

func NewAcknowledge(args *AcknowledgeArgs) Command {
	return Command{Acknowledge, *args}
}

func NewEmptyCommand() Command {
	return Command{Type: EmptyCommand}
}

type Shard struct {
	State ShardState
	Storage map[string]string
}

func NewShard() *Shard {
	return &Shard{ State: Serving, Storage: make(map[string]string) }
}

func CopyShardData(src map[string]string) map[string]string {
	ret := make(map[string]string)
	for k, v := range src {
		ret[k] = v
	}
	return ret
}

func (shard Shard) String() string {
	return fmt.Sprintf("Shard State: %v, Storage: %v", shard.State, shard.Storage)
}

type Result struct {
	Err       Err
	Value     string
}

func (result Result) String() string {
	return fmt.Sprintf("Err: %v, Value: %v", result.Err, result.Value)
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	RequestId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	ConfigNum int
	ShardId []int
}

type PullShardReply struct {
	Err Err
	Num int
	ShardData map[int]map[string]string
	LastRequest map[int64]int64
}

type AcknowledgeArgs struct {
	ConfigNum int
	ShardId []int
}

type AcknowledgeReply struct {
	Err Err
	Num int
}