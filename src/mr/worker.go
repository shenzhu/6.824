package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task, error := AllocateTask()
		if error != nil {
			log.Fatal("Failed to get task from coordinator")
			return
		}

		switch task.TaskType {
		case Map:
			ok := doMapTask(task, mapf)
			if !ok {
				return
			}
		case Reduce:
			ok := doReduceTask(task, reducef)
			if !ok {
				return
			}
		case WAIT:
			doWait()
		case Finished:
			log.Println("All task finished, worker will exit")
			return
		default:
			log.Fatalf("received unrecognized task type %v", task.TaskType)
		}
	}
}

//
// RPC call to get MRTask allocated from coordinator
//
func AllocateTask() (MRTask, error) {
	args := AllocateTaskArgs{}
	reply := AllocateTaskReply{}

	ok := call("Coordinator.AllocateTask", &args, &reply)
	if !ok {
		return MRTask{}, errors.New("get task failed")
	}

	return reply.Task, nil
}

//
// RPC call to report map task finished
//
func reportMapTaskDone(mapIndex int) bool {
	args := ReportMapTaskDoneArgs{MapIndex: mapIndex}
	reply := ReportMapTaskDoneReply{}

	return call("Coordinator.ReportMapTaskDone", &args, &reply)
}

//
// RPC call to report reduce task finished
//
func reportReduceTaskDone(reduceIndex int) bool {
	args := ReportReduceTaskDoneArgs{ReduceIndex: reduceIndex}
	reply := ReportMapTaskDoneReply{}

	return call("Coordinator.ReportReduceTaskDone", &args, &reply)
}

//
// Work on map task
//
func doMapTask(task MRTask, mapf func(string, string) []KeyValue) bool {
	// Extract fields from task
	fileName := task.FileName
	mapIndex := task.MapIndex

	// Read file and compute key-value pairs
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kva := mapf(fileName, string(content))

	// Calculate the reduce index and construct bucket
	buckets := make([][]KeyValue, task.ReduceNumber)
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % task.ReduceNumber
		buckets[reduceIndex] = append(buckets[reduceIndex], kv)
	}

	// Write buckets to immediate file
	for idx, bucket := range buckets {
		immediateFileName := "mr-" + strconv.Itoa(mapIndex) + "-" + strconv.Itoa(idx)

		// Create a temp file and write there
		tempFile, err := ioutil.TempFile("", immediateFileName+"-*")
		if err != nil {
			log.Fatalf("cannot open temp file %v", immediateFileName)
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				break
			}
		}

		// Rename temp file to target name
		os.Rename(tempFile.Name(), immediateFileName)
	}

	log.Printf("Finished processing for map task %d", mapIndex)

	// Send request back to coordinator marking task done
	return reportMapTaskDone(mapIndex)
}

//
// Work on reduce task
//
func doReduceTask(task MRTask, reducef func(string, []string) string) bool {
	// Extract fields from reply
	mapNumber := task.MapNumber
	reduceIdx := task.ReduceIndex

	kva := []KeyValue{}
	for i := 0; i < mapNumber; i++ {
		// Construct file name to read from
		immediateFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceIdx)
		immediataFile, err := os.Open(immediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", immediateFileName)
		}

		// Read KeyValue from immediate files
		dec := json.NewDecoder(immediataFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// Sort
	sort.Sort(ByKey(kva))

	// Create output file to write to
	outputFileName := "mr-out-" + strconv.Itoa(reduceIdx)

	// Create temp file and write there
	tempFile, err := ioutil.TempFile("", outputFileName+"-*")
	if err != nil {
		log.Fatalf("cannot open temp file %v", outputFileName)
	}

	// Aggregate KeyValue with the same key
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// Rename temp file to target name
	os.Rename(tempFile.Name(), outputFileName)

	log.Printf("Finished reduce task, file %s generated, will contact coordinator to report task done", outputFileName)

	// Send requests back to coordinator marking task done
	return reportReduceTaskDone(reduceIdx)
}

//
// Wait
//
func doWait() {
	time.Sleep(3 * time.Second)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
