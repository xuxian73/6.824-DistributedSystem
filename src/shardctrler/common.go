package shardctrler

import "fmt"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func NewConfig() Config {
	return Config{Groups: make(map[int][]string)}
}

const (
	Join = "Join"
	Leave = "Leave"
	Move = "Move"
	Query = "Query"
) 

type Err int8

const (
	OK Err = iota
	ErrWrongLeader
	ErrTimeOut
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeOut:
		return "ErrTimeOut"
	default:
		return "UnknownErr"
	}
}

type Op struct {
	// Your data here.
	Type      string
	Servers   map[int][]string
	Shard     int
	GIDs      []int
	GID       int
	Num       int
	ClientId  int64
	RequestId int64
}

func (op Op) String() string{
	switch op.Type{
	case Join:
		return fmt.Sprintf("%v Servers: %v, ClientId: %v, RequestId: %v", op.Type, op.Servers, op.ClientId, op.RequestId)
	case Leave:
		return fmt.Sprintf("%v GIDs: %v, ClientId: %v, RequestId: %v", op.Type, op.GIDs, op.ClientId, op.RequestId)
	case Move:
		return fmt.Sprintf("%v Shard: %v, GID: %v, ClientId: %v, RequestId: %v", op.Type, op.Shard, op.GID, op.ClientId, op.RequestId)
	case Query:
		return fmt.Sprintf("%v Num: %v, ClientId: %v, RequestId: %v", op.Type, op.Num, op.ClientId, op.RequestId)
	default:
		return "Unknown Operation"
	}
}

type Result struct {
	Err         Err
	Config      Config
	ClientId    int64
	RequestId   int64
}

func (result Result) String() string {
	return fmt.Sprintf("Err: %v, Config: %v, ClientId: %v, RequestId: %v", result.Err, result.Config, result.ClientId, result.RequestId)
}
 
type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientId int64
	RequestId int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ClientId int64
	RequestId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientId int64
	RequestId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ClientId int64
	RequestId int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
