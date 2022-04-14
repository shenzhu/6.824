package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Task type for workers
const (
	Map      int = 0
	Reduce   int = 1
	Finished int = 2
	WAIT     int = 3
)

// Task to allocate to workers
type MRTask struct {
	TaskType     int
	MapNumber    int
	MapIndex     int
	ReduceNumber int
	ReduceIndex  int
	FileName     string
}

type Coordinator struct {
	// Your definitions here.

	// Number of map tasks to use
	nMap int

	// Number of map tasks finished
	nFinishedMap int

	// Number of reduce tasks to use
	nReduce int

	// Number of reduce tasks finished
	nFinishedReduce int

	// Input files
	files []string

	// Status for each map job
	mapStatus []int

	// Status for each reduce job
	reduceStatus []int

	// Mutex
	mu sync.Mutex
}

//
// Constructor for Coordinator
//
func NewCoordinator(files []string, nReduce int) *Coordinator {
	var c Coordinator

	c.nMap = len(files)
	c.nFinishedMap = 0
	c.nReduce = nReduce
	c.nFinishedReduce = 0
	c.files = files
	c.mapStatus = make([]int, c.nMap)
	c.reduceStatus = make([]int, c.nReduce)

	return &c
}

// Your code here -- RPC handlers for the worker to call.

//
// RPC call to get one unprocessed file
//

func (c *Coordinator) AllocateTask(args *AllocateTaskArgs, reply *AllocateTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nFinishedMap < c.nMap {
		// Map task has not finished, allocate map task
		for idx, status := range c.mapStatus {
			if status == 0 {
				// Found one unassigned map task, assign to worker and start monitoring
				reply.Task = MRTask{Map, c.nMap, idx, c.nReduce, -1, c.files[idx]}

				c.mapStatus[idx] = 1

				log.Printf("Allocating map task %d with file %s", idx, c.files[idx])

				// Create a background goroutine to monitor if it can be finished within 10 seconds
				go func() {
					time.Sleep(10 * time.Second)

					c.mu.Lock()
					defer c.mu.Unlock()
					if c.mapStatus[idx] != 3 {
						log.Printf("Haven't received results for map task %d for 10 seconds, will reassign this map task to another worker", idx)
						c.mapStatus[idx] = 0
					}
				}()

				break
			}
		}

		if (reply.Task == MRTask{}) {
			// All map task in progress, ask the worker to wait
			reply.Task = MRTask{WAIT, c.nMap, -1, c.nReduce, -1, ""}
		}

	} else if c.nFinishedMap == c.nMap && c.nFinishedReduce < c.nReduce {
		// All map task finished, allocate reduce task
		for idx, status := range c.reduceStatus {
			if status == 0 {
				// Found one unassigned reduce task, assign to worker and start monitoring
				var task = MRTask{Reduce, c.nMap, -1, c.nReduce, idx, ""}
				reply.Task = task
				c.reduceStatus[idx] = 2

				log.Printf("Allocating reduce task %d", idx)

				// Create a background goroutine to monitor if it can be finished within 10 seconds
				go func() {
					time.Sleep(10 * time.Second)

					c.mu.Lock()
					defer c.mu.Unlock()
					if c.reduceStatus[idx] != 3 {
						log.Printf("Haven't received results for reduce task %d for 10 seconds, will reassign this reduce task to another worker", idx)
						c.reduceStatus[idx] = 0
					}
				}()

				break
			}
		}

		if (reply.Task == MRTask{}) {
			// All reduce task in process, ask the worker to wait
			reply.Task = MRTask{WAIT, c.nMap, -1, c.nReduce, -1, ""}
		}
	} else {
		// All map and reduce task finished, signal the worker to stop
		reply.Task = MRTask{Finished, c.nMap, -1, c.nReduce, -1, ""}
	}

	return nil
}

//
// RPC handler to report map task done
//
func (c *Coordinator) ReportMapTaskDone(args *ReportMapTaskDoneArgs, reply *ReportMapTaskDoneReply) error {
	mapIndex := args.MapIndex

	log.Printf("Received ReportMapTaskDone request for map task %d from worker", mapIndex)

	// Lock before modifying data
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nFinishedMap += 1
	c.mapStatus[mapIndex] = 3

	return nil
}

//
// RPC handler to report reduce task done
//
func (c *Coordinator) ReportReduceTaskDone(args *ReportReduceTaskDoneArgs, reply *ReportReduceTaskDoneReply) error {
	reduceIndex := args.ReduceIndex

	log.Printf("Received ReportReduceTaskDone request for map task %d from worker", reduceIndex)

	// Lock before modifying data
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nFinishedReduce += 1
	c.reduceStatus[reduceIndex] = 3

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nFinishedMap == c.nMap && c.nFinishedReduce == c.nReduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := NewCoordinator(files, nReduce)

	c.server()
	return c
}
