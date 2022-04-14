package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

//
// RPC to allocate task to worker
//
type AllocateTaskArgs struct {
}

type AllocateTaskReply struct {
	Task MRTask
}

//
// RPC to report to coordinator that map task is finished
//
type ReportMapTaskDoneArgs struct {
	MapIndex int
}

type ReportMapTaskDoneReply struct {
}

//
// RPC to report to coordinator that reduce task is finished
type ReportReduceTaskDoneArgs struct {
	ReduceIndex int
}

type ReportReduceTaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
