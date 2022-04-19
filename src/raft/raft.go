package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// State of current Raft
//
const (
	LEADER    int = 0
	FOLLOWER  int = 1
	CANDIDATE int = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// Log entry
//
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// latest term server has been
	currentTerm int

	// candidateId that received vote in current term
	votedFor int

	// logs
	log []LogEntry

	// index of highest log committed
	commitIndex int

	// index of highest log applied
	lastApplied int

	// index of next log entry to send to each server
	nextIndex []int

	// index of highest log entry replicated in each server
	matchIndex []int

	// state of current Raft
	state int

	// timer used for election
	electionTimer *time.Timer

	// timer used for heart beat
	heartbeatTimer *time.Timer

	// conditional variable to signal log replicated to majority
	cond *sync.Cond

	// channel to communicate with KV service
	applyCh chan ApplyMsg

	// timer used to check commit consensus
	commitCheckerTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).

	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	rf.mu.Unlock()

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

//
// Get heartbeat timeout
//
func (rf *Raft) GetHeartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}

//
// Get election timeout of current term
//
func (rf *Raft) GetElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}

//
// Get commit checker timeout
//
func (rf *Raft) GetCommitCheckerTimeout() time.Duration {
	return time.Duration(10) * time.Millisecond
}

//
// Check if we could advance commit index by seeing how many servers the log get replicated to
//
func (rf *Raft) checkCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for idx := rf.commitIndex + 1; idx < len(rf.log); idx++ {
		N := idx

		replicateCount := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			if rf.matchIndex[peer] >= N {
				replicateCount += 1
			}

			if replicateCount > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
				rf.commitIndex = N
				DPrintf("Raft %d received majority at %d, and updated commitIndex to %d", rf.me, N, rf.commitIndex)
				rf.cond.Signal()
				break
			}
		}
	}
}

//
// Send out heartbeat to all peers as leader, the heartbeat(AppendEntries) may contain actual data
//
func (rf *Raft) sendHeartbeat(peer int, term int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommit int) {
	// check if we should send empty AppendEntries
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: leaderCommit,
	}
	DPrintf("Raft %d comparing for %d, %d %d", rf.me, peer, rf.nextIndex[peer], len(rf.log))

	if rf.nextIndex[peer] >= len(rf.log) {
		args.Entries = make([]LogEntry, 0)
	} else {
		args.Entries = []LogEntry{rf.log[rf.nextIndex[peer]]}
	}
	rf.mu.Unlock()

	DPrintf("Raft %d will send AppendEntries request to %d: %s", rf.me, peer, args.ToString())

	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(peer, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("Raft %d received AppendEntries reply: %s", rf.me, reply.ToString())

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			return
		}

		if rf.currentTerm == reply.Term && rf.state == LEADER {
			if !reply.Success {
				DPrintf("Raft %d failed to replicate log to %d, decrementing nextInt to %d", rf.me, peer, rf.nextIndex[peer]-1)
				if reply.ConflictTerm != -1 {
					conflictTermFound := false
					i := len(rf.log) - 1
					for i > 0 {
						if rf.log[i].Term == reply.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							conflictTermFound = true
							break
						}
						i -= 1
					}

					if !conflictTermFound {
						rf.nextIndex[peer] = reply.ConflictIndex
					}
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			} else {
				DPrintf("Raft %d successfully replicated log to %d", rf.me, peer)
				rf.nextIndex[peer] += len(args.Entries)
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			}
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// candidate's term
	Term int

	// candidate requesting vote
	CandidateId int

	// index of candidate's last log entry
	LastLogIndex int

	// term of candidate's last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// currentTerm, for candidate to update itself
	Term int

	// true means candidate received vote
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	currLastLogTerm := rf.log[len(rf.log)-1].Term
	logUpToDate := (args.LastLogTerm > currLastLogTerm) || (args.LastLogTerm == currLastLogTerm && args.LastLogIndex >= len(rf.log))
	if !logUpToDate {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	DPrintf("Raft %d granted vote to %d", rf.me, args.CandidateId)

	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(rf.GetElectionTimeout())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

//
// AppendEntries RPC arguments structure
//
type AppendEntriesArgs struct {
	// learder's term
	Term int

	// leader id
	LeaderId int

	// index of log entry immediately preceding new ones
	PrevLogIndex int

	// term of prevLogIndex entry
	PrevLogTerm int

	// log entries to store
	Entries []LogEntry

	// leader's commitIndex
	LeaderCommit int
}

func (args *AppendEntriesArgs) ToString() string {
	return fmt.Sprintf("[Term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d, Entries=%v, LeaderCommit=%d]",
		args.Term,
		args.LeaderId,
		args.PrevLogIndex,
		args.PrevLogTerm,
		args.Entries,
		args.LeaderCommit)
}

//
// AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
	// current term, for leader to update itself
	Term int

	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool

	// conflict index
	ConflictIndex int

	// conflict term
	ConflictTerm int
}

func (reply *AppendEntriesReply) ToString() string {
	return fmt.Sprintf("[Term=%d, Success=%v]", reply.Term, reply.Success)
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Raft %d received AppendEntries: %s", rf.me, args.ToString())

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("Raft %d rejected AppendEntries because of term: %d < %d", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// another Raft node became leader, update self state to follower and reset election timer
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.electionTimer.Reset(rf.GetElectionTimeout())

	// reply false if log doesn't contain an entry at prevLogIndex matching prevLogTerm
	if len(rf.log) <= args.PrevLogIndex {
		DPrintf("Raft %d rejected AppendEntries because of not having prevLogIndex: len(rf.log)=%d, args.PrevLogIndex=%d", rf.me, len(rf.log), args.PrevLogIndex)
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Raft %d rejected AppendEntries because term %d does not match with %d", rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		j := 0
		for j < len(rf.log) {
			if rf.log[j].Term == reply.ConflictTerm {
				break
			}
			j += 1
		}
		reply.ConflictIndex = j
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// delete and append log
	i := 0
	j := args.PrevLogIndex + 1
	for i = 0; i < len(args.Entries); i++ {
		if j >= len(rf.log) {
			break
		}

		if rf.log[j].Term == args.Entries[i].Term {
			j++
		} else {
			rf.log = append(rf.log[:j], args.Entries[i:]...)
			i = len(args.Entries)
			break
		}
	}

	if i != len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
	}

	DPrintf("Raft %d updated its log to: %v", rf.me, rf.log)

	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64((args.LeaderCommit)), float64(len(rf.log))-1))
		if rf.commitIndex > oldCommitIndex {
			rf.cond.Signal()
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

//
// RPC to send AppendEntries
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if !isLeader {
		return -1, -1, false
	}

	DPrintf("Raft %d received Start command: %v", rf.me, command)

	// append entry to local log
	newLogEntry := LogEntry{Term: term, Command: command}
	rf.log = append(rf.log, newLogEntry)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// Background process checking if we can apply logs to state machine
//
func (rf *Raft) logApplier() {
	for {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			DPrintf("Raft %d ready to apply state machine: %v at %d, commitIndex: %d", rf.me, rf.log, rf.lastApplied, rf.commitIndex)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
		} else {
			DPrintf("Raft %d started waiting log apply", rf.me)
			rf.cond.Wait()
			DPrintf("Raft %d ended waiting log apply", rf.me)
			rf.mu.Unlock()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()

			if rf.state != LEADER {
				// update state to prepare for election
				rf.currentTerm += 1
				rf.state = CANDIDATE
				rf.votedFor = rf.me

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}

				DPrintf("Raft %d started RequestVote %v", rf.me, args)

				voteGranted := 1
				for idx := range rf.peers {
					if idx == rf.me {
						continue
					}

					// send out requests
					go func(peer int) {
						reply := RequestVoteReply{}

						DPrintf("Raft %d sending RequestVote to %d", rf.me, peer)
						if rf.sendRequestVote(peer, &args, &reply) {
							// check reply and aggregate results
							rf.mu.Lock()
							if rf.currentTerm == args.Term && rf.state == CANDIDATE {
								if reply.VoteGranted {
									voteGranted += 1
									if voteGranted > len(rf.peers)/2 {
										DPrintf("Raft %d received majority vote %d, will become leader", rf.me, voteGranted)
										rf.state = LEADER

										// initialize all nextIndex to the index after the one in its log
										for idx := range rf.nextIndex {
											rf.nextIndex[idx] = len(rf.log)
											rf.matchIndex[idx] = 0
										}

										// send out heartbeat to all peers to establish authority
										go rf.sendHeartbeat(peer, rf.currentTerm, rf.me, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1].Term, rf.commitIndex)
										rf.heartbeatTimer.Reset(rf.GetHeartbeatTimeout())

										// start checking commit
										go rf.checkCommit()
										rf.commitCheckerTimer.Reset(rf.GetCommitCheckerTimeout())
									}
								} else if reply.Term > rf.currentTerm {
									// current term out of date, reverts to follower state
									rf.state = FOLLOWER
									rf.currentTerm = reply.Term
									rf.votedFor = -1
								}
							}
							rf.mu.Unlock()
						}
					}(idx)
				}
			}

			rf.electionTimer.Reset(rf.GetElectionTimeout())

			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == LEADER {
				for peer := range rf.peers {
					if peer == rf.me {
						continue
					}

					go rf.sendHeartbeat(peer, rf.currentTerm, rf.me, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1].Term, rf.commitIndex)
				}
				// batchSize := len(rf.log) - 1 - rf.commitIndex
				// go rf.sendHeartbeat(rf.currentTerm, rf.me, len(rf.log)-1-batchSize, rf.log[len(rf.log)-1-batchSize].Term, rf.commitIndex)
				rf.heartbeatTimer.Reset(rf.GetHeartbeatTimeout())
			}
			rf.mu.Unlock()
		case <-rf.commitCheckerTimer.C:
			rf.mu.Lock()
			if rf.state == LEADER {
				go rf.checkCommit()
				rf.commitCheckerTimer.Reset(rf.GetCommitCheckerTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = FOLLOWER

	// set election timer and heartbeat timer
	rf.heartbeatTimer = time.NewTimer(rf.GetHeartbeatTimeout())
	rf.electionTimer = time.NewTimer(rf.GetElectionTimeout())

	rf.cond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	rf.commitCheckerTimer = time.NewTimer(rf.GetCommitCheckerTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start log applier goroutine
	go rf.logApplier()

	return rf
}
