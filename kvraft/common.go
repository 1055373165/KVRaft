package kvraft

import (
	"fmt"
	"time"
)

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
	Op    string // Put or Append
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

const ClientRequestTimeout = 500 * time.Millisecond

// 传递给 raft 的数据结构
type Op struct {
	// Your definition here
	// Field names must start with capital letters
	// otherwire RPC will break
	Key      string
	Value    string
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

type OpReply struct {
	Value string
	Err   Err
}

// PutAppend supports both Put and Append operations
func getOperationType(v string) OperationType {
	switch v {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic(fmt.Sprintf("unknown operation type %s", v))
	}
}

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}
