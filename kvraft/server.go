package kvraft

import (
	"bytes"
	"kvraft1/labgob"
	"kvraft1/labrpc"
	"kvraft1/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied  int // avoid repeated application
	stateMachine *MemoryKVStateMachine
	notifyChans  map[int]chan *OpReply

	duplicateTable map[int64]LastOperationInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// store the request in the raft log and synchronize it
	index, _, isLeader := kv.rf.Start(Op{
		Key:    args.Key,
		OpType: OpGet,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// await for the result
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannle(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}
	// remove channel operation should not be allowed to block returning the result to the client.
	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.removeNotifyChannel(index)
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		// 重复请求
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   getOperationType(args.Op),
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// await for the result
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannle(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.removeNotifyChannel(index)
	}()
}

func (kv *KVServer) requestDuplicated(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer servers[] contains the ports of the set of
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

	// You may need initialization code here.
	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)

	// restore the state from a snapshot
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	return kv
}

// applyTask is the main loop for the KVServer. It waits for
// messages from the Raft instance and applies them to the state machine.
// It also sends the results of the operations back to the client.
func (kv *KVServer) applyTask() {
	for !kv.killed() {
		message := <-kv.applyCh
		if message.CommandValid {
			kv.mu.Lock()
			// 如果是已经处理过的消息则直接忽略
			if message.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = message.CommandIndex

			// retrieve user action information
			op := message.Command.(Op)
			var opReply *OpReply
			if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
				// read directly from the cache
				opReply = kv.duplicateTable[op.ClientId].Reply
			} else {
				// 将操作应用到我们的状态机中（单机 KV）返回应用结果
				opReply = kv.applyToStateMachine(op)
				// 将结果暂时缓存起来
				if op.OpType != OpGet {
					kv.duplicateTable[op.ClientId] = LastOperationInfo{
						SeqId: op.SeqId,
						Reply: opReply,
					}
				}
			}

			// 将应用结果发送回去
			if _, isLeader := kv.rf.GetState(); isLeader {
				notifyCh := kv.getNotifyChannle(message.CommandIndex)
				notifyCh <- opReply
			}

			// 判断是否需要 snapshot 如果 raftstate 数据大小已经超过了设定的阈值
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				kv.makeSnapshot(message.CommandIndex)
			}
			kv.mu.Unlock()
		} else if message.SnapshotValid {
			kv.mu.Lock()
			kv.restoreFromSnapshot(message.Snapshot)
			kv.lastApplied = message.SnapshotIndex
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		kv.stateMachine.Append(op.Key, op.Value)
	}
	return &OpReply{
		Value: value,
		Err:   err,
	}
}

func (kv *KVServer) getNotifyChannle(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) removeNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	_ = enc.Encode(kv.stateMachine)
	_ = enc.Encode(kv.duplicateTable)
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *KVServer) restoreFromSnapshot(snapShot []byte) {
	if len(snapShot) == 0 {
		return
	}

	buf := bytes.NewBuffer(snapShot)
	dec := labgob.NewDecoder(buf)
	var stateMachine MemoryKVStateMachine
	var duplicateTable map[int64]LastOperationInfo
	if dec.Decode(&stateMachine) != nil || dec.Decode(&duplicateTable) != nil {
		panic("failed to resotre state from snapshot")
	}

	kv.stateMachine = &stateMachine
	kv.duplicateTable = duplicateTable
}
