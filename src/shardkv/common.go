package shardkv

import "fmt"

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

type Result struct {
	Err       Err
	Value     string
	ClientId  int64
	RequestId int64
}

func (result Result) String() string {
	return fmt.Sprintf("Err: %v, Value: %v, ClientId: %v, RequestId: %v", result.Err, result.Value, result.ClientId, result.RequestId)
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
