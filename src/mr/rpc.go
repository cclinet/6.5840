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

// example to show how to declare the arguments
// and reply for an RPC.
const (
	RequestNewTask     = 1
	ReportTaskComplete = 2
)

type ReportTaskCompleteArgs struct {
	TaskType int
	TaskID   int
}

type RequestArgs struct {
	Type                   int
	ReportTaskCompleteArgs ReportTaskCompleteArgs
}

const (
	Map      = 1
	Reduce   = 2
	Continue = 3
	OK       = 3
	Stop     = 4
)

type MapArgs struct {
	TaskID        int
	FileName      string
	ReduceTaskNum int
}
type ReduceArgs struct {
	TaskID        int
	MapTaskNum    int
	ReduceTaskNum int
}
type RPCResponseArgs struct {
	Type   int
	Map    MapArgs
	Reduce ReduceArgs
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
