package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

const (
	OPTYPE_GET    = 0
	OPTYPE_PUT    = 1
	OPTYPE_APPEND = 2
)

const TIMEOUT = 500 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		prefix := fmt.Sprintf("%s    ", time.Now())
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	OpType    int
	ClientId  int
	RequestId int64
}

func (op *Op) ToString() string {
	return fmt.Sprintf("[Key=%s, Value=%s, Optype=%d, Clientid=%d, RequestId=%d]",
		op.Key,
		op.Value,
		op.OpType,
		op.ClientId,
		op.RequestId,
	)
}

type Reply struct {
	Err       Err
	Value     string
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// last applied index
	lastApplied int

	// in-memory key-value map
	storage map[string]string

	// channels for inflight request
	inflightChans map[int]chan *Reply

	// last operation of each client
	lastOperations map[int]*Reply
}

//
// Check if Op is duplicate
//
func (kv *KVServer) checkDuplicate(op *Op) bool {
	if val, ok := kv.lastOperations[op.ClientId]; ok {
		return val.RequestId == op.RequestId
	}
	return false
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	var lastOperations map[int]*Reply
	var storage map[string]string

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&lastOperations); e != nil {
		lastOperations = map[int]*Reply{}
	}
	if e := d.Decode(&storage); e != nil {
		storage = make(map[string]string)
	}

	kv.lastOperations = lastOperations
	kv.storage = storage
}

//
// Remove outdated inflight channels
//
func (kv *KVServer) removeOutdatedChan(index int) {
	// garbage collection only when more than 5 to save work
	if len(kv.inflightChans) < 5 {
		return
	}

	for i := 0; i < index; i++ {
		delete(kv.inflightChans, i)
	}
}

//
// Background goroutine listening at applyCh to get Raft committed messages
//
func (kv *KVServer) applier() {
	for message := range kv.applyCh {
		if kv.killed() {
			return
		}

		DPrintf("Server %d Raft committed message %v", kv.me, message)

		if message.CommandValid {
			kv.mu.Lock()
			if message.CommandIndex <= kv.lastApplied {
				continue
			}
			kv.lastApplied = message.CommandIndex
			kv.mu.Unlock()

			op := message.Command.(Op)
			reply := Reply{RequestId: op.RequestId}
			if op.OpType != OPTYPE_GET && kv.checkDuplicate(&op) {
				// found duplicate
				DPrintf("Server %d found duplicate request: %s", kv.me, op.ToString())
				reply = *kv.lastOperations[op.ClientId]
			} else if op.OpType == OPTYPE_GET {
				DPrintf("Server %d applying Get: %s", kv.me, op.ToString())
				result, ok := kv.storage[op.Key]
				if ok {
					reply.Value = result
					reply.Err = OK
				} else {
					reply.Err = ErrNoKey
				}
			} else if op.OpType == OPTYPE_PUT {
				DPrintf("Server %d applying Put: %s", kv.me, op.ToString())
				kv.storage[op.Key] = op.Value
				reply.Err = OK

				kv.lastOperations[op.ClientId] = &reply
			} else if op.OpType == OPTYPE_APPEND {
				DPrintf("Server %d applying Append: %s", kv.me, op.ToString())
				result, ok := kv.storage[op.Key]
				if ok {
					kv.storage[op.Key] = result + op.Value
				} else {
					kv.storage[op.Key] = op.Value
				}
				reply.Err = OK

				kv.lastOperations[op.ClientId] = &reply
			}

			_, isLeader := kv.rf.GetState()
			if isLeader {
				DPrintf("Server %d is leader, sending message back to index %d", kv.me, message.CommandIndex)
				kv.mu.Lock()
				ch := kv.getInflightChan(message.CommandIndex)
				kv.mu.Unlock()

				ch <- &reply
			}

			if kv.rf.GetStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
				DPrintf("Server %d creating snapshot: kv.rf.GetStateSize(%d) >= kv.maxraftstate(%d)", kv.me, kv.rf.GetStateSize(), kv.maxraftstate)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.lastOperations)
				e.Encode(kv.storage)
				kv.rf.Snapshot(message.CommandIndex, w.Bytes())
			}
		} else {
			ok := kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)
			DPrintf("Server %d calling CondInstallSnapshot", kv.me)
			if ok {
				kv.lastApplied = message.SnapshotIndex
				kv.readSnapshot(message.Snapshot)
			}
		}
	}
}

//
// Get associated inflight channel for given index
//
func (kv *KVServer) getInflightChan(index int) chan *Reply {
	if _, ok := kv.inflightChans[index]; !ok {
		// https://stackoverflow.com/questions/23233381/whats-the-difference-between-c-makechan-int-and-c-makechan-int-1
		kv.inflightChans[index] = make(chan *Reply, 1)
	}

	return kv.inflightChans[index]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Key: args.Key, OpType: OPTYPE_GET}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Server %d received Get request with index %d: %s", kv.me, index, args.ToString())

	kv.mu.Lock()
	ch := kv.getInflightChan(index)
	kv.mu.Unlock()

	select {
	case resp := <-ch:
		reply.Value = resp.Value
		reply.Err = resp.Err
		break
	case <-time.After(TIMEOUT):
		DPrintf("Server %d timed out for Get: %s", kv.me, args.ToString())
		reply.Err = ErrTimeout
		break
	}

	go func() {
		kv.mu.Lock()
		kv.removeOutdatedChan(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if args.Op == "Put" {
		op.OpType = OPTYPE_PUT
	} else if args.Op == "Append" {
		op.OpType = OPTYPE_APPEND
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("Server %d received PutAppend request: %s, but not leader", kv.me, args.ToString())
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Server %d received PutAppend request with index %d: %s", kv.me, index, args.ToString())

	kv.mu.Lock()
	ch := kv.getInflightChan(index)
	kv.mu.Unlock()

	select {
	case resp := <-ch:
		reply.Err = resp.Err
		break
	case <-time.After(TIMEOUT):
		DPrintf("Server %d timed out for PutAppend: %s", kv.me, args.ToString())
		reply.Err = ErrTimeout
		break
	}

	go func() {
		kv.mu.Lock()
		kv.removeOutdatedChan(index)
		kv.mu.Unlock()
	}()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
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
//
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
	kv.lastApplied = 0
	kv.inflightChans = make(map[int]chan *Reply)
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier()

	return kv
}
