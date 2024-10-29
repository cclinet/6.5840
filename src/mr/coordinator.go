package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type MapTask struct {
	filename  string
	completed bool
}
type ReduceTask struct {
	partition int
	completed bool
}

type Coordinator struct {
	// Your definitions here.
	mapTask                []MapTask
	reduceTask             []ReduceTask
	mapChan                chan int
	reduceChan             chan int
	mapTaskNum             int
	reduceTaskNum          int
	completedMapTaskNum    int
	completedReduceTaskNum int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Call(req *RequestArgs, res *RPCResponseArgs) error {
	switch req.Type {
	case RequestNewTask:
		if c.completedReduceTaskNum == c.reduceTaskNum {
			res.Type = Stop
			goto exit
		}
		if c.completedMapTaskNum == c.mapTaskNum {
			taskID := <-c.reduceChan
			res.Type = Reduce
			res.Reduce = ReduceArgs{TaskID: taskID, MapTaskNum: c.mapTaskNum}
			// reduce
			goto exit
		}
		if len(c.mapChan) == 0 {
			res.Type = Continue
			goto exit
		}
		{
			taskID := <-c.mapChan
			res.Type = Map
			res.Map = MapArgs{TaskID: taskID, FileName: c.mapTask[taskID].filename, ReduceTaskNum: c.reduceTaskNum}
			go func() {
				time.AfterFunc(10*time.Second, func() {
					if !c.mapTask[taskID].completed {
						c.mapChan <- taskID
					}
				})
			}()
			goto exit
		}
	case ReportTaskComplete:
		taskID := req.ReportTaskCompleteArgs.TaskID
		switch req.ReportTaskCompleteArgs.TaskType {
		case Map:
			c.mapTask[taskID].completed = true
			c.completedMapTaskNum += 1
		case Reduce:
			c.reduceTask[taskID].completed = true
			c.completedReduceTaskNum += 1
		}
		res.Type = OK
	}
exit:
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := (c.completedReduceTaskNum == c.reduceTaskNum)

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.reduceTaskNum = nReduce
	c.reduceChan = make(chan int, c.reduceTaskNum)
	c.completedReduceTaskNum = 0
	for i := range nReduce {
		c.reduceTask = append(c.reduceTask, ReduceTask{partition: i, completed: false})
		c.reduceChan <- i
	}
	c.mapTaskNum = len(files)
	c.mapChan = make(chan int, c.mapTaskNum)
	c.completedMapTaskNum = 0
	for i, file := range files {
		c.mapTask = append(c.mapTask, MapTask{filename: file, completed: false})
		c.mapChan <- i
	}

	// Your code here.

	c.server()
	return &c
}
