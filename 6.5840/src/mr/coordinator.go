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

type State int

const (
	Waiting State = iota
	Running
	Done
)

type MapTask struct {
	taskId    int
	inputFile string
	state     State
}

type ReduceTask struct {
	taskId int
	state  State
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	nReduce     int
	mapTasks    []MapTask
	reduceTasks []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.IsMapTaskDone() && c.IsReduceTaskDone()
}

func (c *Coordinator) IsMapTaskDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.mapTasks {
		if task.state != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) IsReduceTaskDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.reduceTasks {
		if task.state != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	if c.IsMapTaskDone() {
		reply.Finished = true
		return nil
	}
	reply.WaitJobs = true
	for i := range c.mapTasks {
		if c.mapTasks[i].state == Waiting {
			c.mu.Lock()
			c.mapTasks[i].state = Running
			c.mu.Unlock()
			go c.crashMapWaiter(i)

			reply.TaskId = c.mapTasks[i].taskId
			reply.InputFile = c.mapTasks[i].inputFile
			reply.NReduce = c.nReduce
			reply.Finished = false
			reply.WaitJobs = false
			return nil
		}
	}
	return nil
}

func (c *Coordinator) crashMapWaiter(taskid int) {
	time.Sleep(10 * time.Second)
	if c.mapTasks[taskid].state == Running {
		c.mu.Lock()
		c.mapTasks[taskid].state = Waiting
		c.mu.Unlock()
	}
}

func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	if c.IsReduceTaskDone() {
		reply.Finished = true
		return nil
	}
	reply.WaitJobs = true
	for i := range c.reduceTasks {
		if c.reduceTasks[i].state == Waiting {
			c.mu.Lock()
			c.reduceTasks[i].state = Running
			c.mu.Unlock()
			go c.crashReduceWaiter(i)

			reply.TaskId = c.reduceTasks[i].taskId
			reply.NMap = len(c.mapTasks)
			reply.Finished = false
			reply.WaitJobs = false
			return nil
		}
	}
	return nil
}

func (c *Coordinator) crashReduceWaiter(taskid int) {
	time.Sleep(10 * time.Second)
	if c.reduceTasks[taskid].state == Running {
		c.mu.Lock()
		c.reduceTasks[taskid].state = Waiting
		c.mu.Unlock()
	}
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	for i := range c.mapTasks {
		if c.mapTasks[i].taskId == args.TaskId {
			c.mu.Lock()
			c.mapTasks[i].state = Done
			c.mu.Unlock()
			break
		}
	}
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].taskId == args.TaskId {
			c.mu.Lock()
			c.reduceTasks[i].state = Done
			c.mu.Unlock()
			break
		}
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce

	c.mapTasks = make([]MapTask, len(files))
	for index, file := range files {
		c.mapTasks[index] = MapTask{inputFile: file, taskId: index, state: Waiting}
	}

	c.reduceTasks = make([]ReduceTask, nReduce)
	for index := range nReduce {
		c.reduceTasks[index] = ReduceTask{taskId: index, state: Waiting}
	}

	c.server()
	return &c
}
