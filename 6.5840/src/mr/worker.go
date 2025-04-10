package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	DoMapTask(mapf)
	DoReduceTask(reducef)
}

func DoMapTask(mapf func(string, string) []KeyValue) {
	for {
		args := GetMapTaskArgs{}
		reply := GetMapTaskReply{}
		ok := call("Coordinator.GetMapTask", &args, &reply)
		if !ok {
			continue
		}
		if reply.Finished {
			return
		}
		if reply.WaitJobs {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		filename := reply.InputFile
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))

		WriteIntermediate(kva, reply.TaskId, reply.NReduce)

		doneArgs := MapTaskDoneArgs{reply.TaskId}
		doneReply := MapTaskDoneReply{}
		call("Coordinator.MapTaskDone", &doneArgs, &doneReply)
	}
}

func WriteIntermediate(kva []KeyValue, mapTaskId int, nReduce int) {
	encs := make([]*json.Encoder, nReduce)
	for i := range nReduce {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskId, i)
		file, _ := os.Create(filename)
		enc := json.NewEncoder(file)
		encs[i] = enc
	}

	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		encs[index].Encode(&kv)
	}
}

func DoReduceTask(reducef func(string, []string) string) {
	for {
		args := GetReduceTaskArgs{}
		reply := GetReduceTaskReply{}
		ok := call("Coordinator.GetReduceTask", &args, &reply)
		if !ok {
			continue
		}
		if reply.Finished {
			return
		}
		if reply.WaitJobs {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		reduceTaskId := reply.TaskId
		kva := ReadIntermediate(reduceTaskId, reply.NMap)
		sort.Sort(ByKey(kva))

		oname := fmt.Sprintf("mr-out-%d", reduceTaskId)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}

		ofile.Close()

		doneArgs := ReduceTaskDoneArgs{reduceTaskId}
		doneReply := ReduceTaskDoneReply{}
		call("Coordinator.ReduceTaskDone", &doneArgs, &doneReply)
	}
}

func ReadIntermediate(reduceTaskId int, nMap int) []KeyValue {
	kva := []KeyValue{}
	decs := make([]*json.Decoder, nMap)
	for i := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTaskId)
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		decs[i] = dec
	}
	for _, dec := range decs {
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
