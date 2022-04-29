package kvraft

import (
	"crypto/rand"
	"math/big"
	mathrand "math/rand"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

const RETRY_INTERVAL = 500 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// id of client
	cid int

	// leader index as know of
	leaderIndex int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.cid = int(mathrand.Int31())
	ck.leaderIndex = 0
	return ck
}

//
// Update Clerk to get the next leader
//
func (ck *Clerk) getNextLeader(prevLeader int32) {
	leader := (prevLeader + 1) % int32(len(ck.servers))
	atomic.StoreInt32(&ck.leaderIndex, leader)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key}
	reply := GetReply{}
	for {
		leader := atomic.LoadInt32(&ck.leaderIndex)

		DPrintf("Clerk %d sending Get to server %d: %s", ck.cid, leader, args.ToString())

		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)

		DPrintf("Clerk %d received Get reply from server %d: %v", ck.cid, leader, reply)

		if ok {
			if reply.Err == OK {
				break
			} else if reply.Err == ErrTimeout {
				continue
			} else if reply.Err == ErrNoKey {
				reply.Value = ""
				break
			}
		}

		ck.getNextLeader(leader)
		time.Sleep(RETRY_INTERVAL)
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.cid,
		RequestId: nrand(),
	}
	reply := PutAppendReply{}

	for {
		leader := atomic.LoadInt32(&ck.leaderIndex)

		DPrintf("Clert %d sending PutAppend to server %d: %s", ck.cid, leader, args.ToString())

		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)

		DPrintf("Clerk %d received PutAppend reply from server %d: %v, status: %v", ck.cid, leader, reply, ok)

		if ok {
			if reply.Err == OK {
				return
			} else if reply.Err == ErrTimeout {
				continue
			}
		}

		ck.getNextLeader(leader)
		time.Sleep(RETRY_INTERVAL)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
