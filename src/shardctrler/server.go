package shardctrler

import (
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

const (
	OPTYPE_JOIN  = 0
	OPTYPE_LEAVE = 1
	OPTYPE_MOVE  = 2
	OPTYPE_QUERY = 3
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

func maxmin(ct map[int][]int, gs []int) (max int, maxi int, min int, mini int) {
	max = 0
	min = math.MaxInt
	for _, g := range gs {
		l := len(ct[g])
		if l > max {
			max = l
			maxi = g
		}
		if l < min {
			min = l
			mini = g
		}
	}
	if len(ct[0]) > 0 { // empty ct[0] before doing anything else
		if min == math.MaxInt {
			return 0, 0, 0, 0
		} else {
			return len(ct[0]) * 2, 0, 0, mini
		}
	} else {
		return max, maxi, min, mini
	}
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	// channels for inflight request
	inflightChans map[int]chan *Reply

	// last operation of each client
	lastOperations map[int]*Reply

	// last applied index
	lastApplied int
}

type Op struct {
	// Your data here.

	// join args
	Servers map[int][]string

	// leave args
	GIDs []int

	// move args
	Shard int
	GID   int

	// query args
	Num int

	// op type
	OpType int

	// for dedup
	ClientId  int
	RequestId int64
}

func (op *Op) ToString() string {
	return fmt.Sprintf("[Servers=%v,GIDs=%v,Shard=%d,GID=%d,Num=%d,OpType=%d,ClientId=%d,RequestId=%d]",
		op.Servers,
		op.GIDs,
		op.Shard,
		op.GID,
		op.Num,
		op.OpType,
		op.ClientId,
		op.RequestId)
}

type Reply struct {
	WrongLeader bool
	Err         Err
	Config      Config
	RequestId   int64
}

//
// Check if Op is duplicate
//
func (sc *ShardCtrler) checkDuplicate(op *Op) bool {
	if val, ok := sc.lastOperations[op.ClientId]; ok {
		return val.RequestId == op.RequestId
	}
	return false
}

//
// Remove outdated inflight channels
//
func (sc *ShardCtrler) removeOutdatedChan(index int) {
	// garbage collection only when more than 5 to save work
	if len(sc.inflightChans) < 5 {
		return
	}

	for i := 0; i < index; i++ {
		delete(sc.inflightChans, i)
	}
}

//
// Get associated inflight channel for given index
//
func (sc *ShardCtrler) getInflightChan(index int) chan *Reply {
	if _, ok := sc.inflightChans[index]; !ok {
		// https://stackoverflow.com/questions/23233381/whats-the-difference-between-c-makechan-int-and-c-makechan-int-1
		sc.inflightChans[index] = make(chan *Reply, 1)
	}

	return sc.inflightChans[index]
}

func (sc *ShardCtrler) rebalance(conf *Config) {
	ct := map[int][]int{0: {}} // gid -> shard
	var gs []int
	for gid := range conf.Groups {
		ct[gid] = []int{}
		gs = append(gs, gid)
	}
	sort.Ints(gs)
	for i, gid := range conf.Shards {
		ct[gid] = append(ct[gid], i)
	}
	for max, maxi, min, mini := maxmin(ct, gs); max-min > 1; max, maxi, min, mini = maxmin(ct, gs) {
		c := (max - min) / 2
		ts := ct[maxi][0:c]
		ct[maxi] = ct[maxi][c:]
		ct[mini] = append(ct[mini], ts...)
		for _, t := range ts {
			conf.Shards[t] = mini
		}
	}
}

func (sc *ShardCtrler) getConfig() Config {
	var conf Config
	conf.Groups = make(map[int][]string)

	lastConfig := sc.configs[len(sc.configs)-1]
	conf.Num = lastConfig.Num + 1
	for k, v := range lastConfig.Groups {
		conf.Groups[k] = v
	}
	for i, s := range lastConfig.Shards {
		conf.Shards[i] = s
	}
	return conf
}

func (sc *ShardCtrler) handleQuery(op *Op, reply *Reply) {
	if op.Num >= 0 && op.Num < len(sc.configs) {
		reply.Config = sc.configs[op.Num]
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.Config = sc.configs[len(sc.configs)-1]
		reply.WrongLeader = false
		reply.Err = OK
	}

	DPrintf("Controller %d sending Query results back: %v", sc.me, *reply)
}

func (sc *ShardCtrler) handleJoin(op *Op, reply *Reply) {
	conf := sc.getConfig()
	for gid, servers := range op.Servers {
		conf.Groups[gid] = servers
	}
	sc.rebalance(&conf)
	sc.mu.Lock()
	sc.configs = append(sc.configs, conf)
	sc.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK
}

func (sc *ShardCtrler) handleLeave(op *Op, reply *Reply) {
	conf := sc.getConfig()
	for _, k := range op.GIDs {
		for i, gid := range conf.Shards {
			if gid == k {
				conf.Shards[i] = 0
			}
		}
		delete(conf.Groups, k)
	}
	sc.rebalance(&conf)

	sc.mu.Lock()
	sc.configs = append(sc.configs, conf)
	sc.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK
}

func (sc *ShardCtrler) handleMove(op *Op, reply *Reply) {
	conf := sc.getConfig()
	conf.Shards[op.Shard] = op.GID

	sc.mu.Lock()
	sc.configs = append(sc.configs, conf)
	sc.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK
}

func (sc *ShardCtrler) handleOps(op *Op, reply *Reply) {
	isDuplicate := sc.checkDuplicate(op)
	if isDuplicate {
		reply = sc.lastOperations[op.ClientId]
		return
	}

	switch op.OpType {
	case OPTYPE_JOIN:
		sc.handleJoin(op, reply)
	case OPTYPE_LEAVE:
		sc.handleLeave(op, reply)
	case OPTYPE_MOVE:
		sc.handleMove(op, reply)
	case OPTYPE_QUERY:
		sc.handleQuery(op, reply)
	}
}

//
// Background goroutine listening at applyCh to get Raft committed messages
//
func (sc *ShardCtrler) applier() {
	for message := range sc.applyCh {
		DPrintf("Controller %d Raft committed message %v", sc.me, message)

		if message.CommandValid {
			sc.mu.Lock()
			if message.CommandIndex <= sc.lastApplied {
				continue
			}
			sc.lastApplied = message.CommandIndex
			sc.mu.Unlock()

			op := message.Command.(Op)
			reply := Reply{RequestId: op.RequestId}
			sc.handleOps(&op, &reply)

			_, isLeader := sc.rf.GetState()
			if isLeader {
				sc.mu.Lock()
				ch := sc.inflightChans[message.CommandIndex]
				sc.mu.Unlock()

				DPrintf("Controller %d sending reply back: %v", sc.me, reply)

				go func() {
					ch <- &reply
				}()
			}
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	op := Op{
		Servers:   args.Servers,
		OpType:    OPTYPE_JOIN,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	DPrintf("Controller %d received Join request: %s", sc.me, op.ToString())

	genericReply := Reply{}
	sc.requestHandler(&op, &genericReply)
	reply.WrongLeader = genericReply.WrongLeader
	reply.Err = genericReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	op := Op{
		GIDs:      args.GIDs,
		OpType:    OPTYPE_LEAVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	DPrintf("Controller %d received Leave request: %s", sc.me, op.ToString())

	genericReply := Reply{}
	sc.requestHandler(&op, &genericReply)
	reply.WrongLeader = genericReply.WrongLeader
	reply.Err = genericReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	op := Op{
		Shard:     args.Shard,
		GID:       args.GID,
		OpType:    OPTYPE_MOVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	DPrintf("Controller %d received Move request: %s", sc.me, op.ToString())

	genericReply := Reply{}
	sc.requestHandler(&op, &genericReply)
	reply.WrongLeader = genericReply.WrongLeader
	reply.Err = genericReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	op := Op{
		Num:       args.Num,
		OpType:    OPTYPE_QUERY,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	DPrintf("Controller %d received Query request: %s", sc.me, op.ToString())

	genericReply := Reply{}
	sc.requestHandler(&op, &genericReply)
	reply.WrongLeader = genericReply.WrongLeader
	reply.Err = genericReply.Err
	reply.Config = genericReply.Config
}

func (sc *ShardCtrler) requestHandler(op *Op, reply *Reply) {
	index, _, isLeader := sc.rf.Start(*op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := sc.getInflightChan(index)
	sc.mu.Unlock()

	reply.WrongLeader = false
	select {
	case resp := <-ch:
		reply.Err = resp.Err
		reply.Config = resp.Config
		break
	case <-time.After(TIMEOUT):
		break
	}

	go func() {
		sc.mu.Lock()
		sc.removeOutdatedChan(index)
		sc.mu.Unlock()
	}()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
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

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.inflightChans = make(map[int]chan *Reply)

	go sc.applier()

	return sc
}
