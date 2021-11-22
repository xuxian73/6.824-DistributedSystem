package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut	   = "ErrTimerOut"
)
const (
	GetOp		   = "Get"
	PutOp		   = "Put"
	AppendOp	   = "Append"
)
type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
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
	Key string
	ClientId  int64
	RequestId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type Result struct {
	Err   Err
	Value string
	ClientId int64
	RequestId int64
}