package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int
	RequestId int64
}

func (args *PutAppendArgs) ToString() string {
	return fmt.Sprintf("[Key=%s, Value=%s, Op=%s, ClientId=%d, Requestid=%d]",
		args.Key,
		args.Value,
		args.Op,
		args.ClientId,
		args.RequestId)
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

func (args *GetArgs) ToString() string {
	return fmt.Sprintf("[Key=%s]", args.Key)
}

type GetReply struct {
	Err   Err
	Value string
}
