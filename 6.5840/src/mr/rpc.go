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
type (
	GetMapTaskArgs  struct{}
	GetMapTaskReply struct {
		TaskId    int
		InputFile string
		NReduce   int
		Finished  bool
		WaitJobs  bool
	}
	GetReduceTaskArgs  struct{}
	GetReduceTaskReply struct {
		TaskId   int
		NMap     int
		Finished bool
		WaitJobs bool
	}
	MapTaskDoneArgs struct {
		TaskId int
	}
	MapTaskDoneReply   struct{}
	ReduceTaskDoneArgs struct {
		TaskId int
	}
	ReduceTaskDoneReply struct{}
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
