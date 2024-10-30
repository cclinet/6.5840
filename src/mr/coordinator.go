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
	mapChan                chan int //需要执行的map任务taskID
	reduceChan             chan int //需要执行的reduce任务taskID
	completedMapTaskNum    int
	completedReduceTaskNum int
	reportChan             chan ReportTaskCompleteArgs //完成任务的记录
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
	// log.Printf("%v", req)
	switch req.Type {
	case RequestNewTask:
		if c.completedReduceTaskNum == len(c.reduceTask) {
			res.Type = Stop
			return nil
		}
		if c.completedMapTaskNum == len(c.mapTask) {
			select {
			case taskID := <-c.reduceChan:
				res.Type = Reduce
				res.Reduce = ReduceArgs{TaskID: taskID, MapTaskNum: len(c.mapTask)}
				go func() {
					time.AfterFunc(10*time.Second, func() {
						if !c.reduceTask[taskID].completed {
							c.reduceChan <- taskID
						}
					})
				}()
			default:
				res.Type = Continue
			}

			return nil
		}

		select {
		case taskID := <-c.mapChan:
			res.Type = Map
			res.Map = MapArgs{TaskID: taskID, FileName: c.mapTask[taskID].filename, ReduceTaskNum: len(c.reduceTask)}
			go func() {
				time.AfterFunc(10*time.Second, func() {
					if !c.mapTask[taskID].completed {
						c.mapChan <- taskID
					}
				})
			}()
		default:
			res.Type = Continue
		}
		return nil
	case ReportTaskComplete:
		c.reportChan <- req.ReportTaskCompleteArgs
		res.Type = OK
	}

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
	ret := (c.completedReduceTaskNum == len(c.reduceTask))

	time.Sleep(1 * time.Second)
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 初始化reduce任务
	c.completedReduceTaskNum = 0
	c.reduceChan = make(chan int, nReduce)
	for i := range nReduce {
		c.reduceTask = append(c.reduceTask, ReduceTask{partition: i, completed: false})
		c.reduceChan <- i
	}

	// 初始化map任务
	c.completedMapTaskNum = 0
	c.mapChan = make(chan int, len(files))
	for i, file := range files {
		c.mapTask = append(c.mapTask, MapTask{filename: file, completed: false})
		c.mapChan <- i
	}

	c.reportChan = make(chan ReportTaskCompleteArgs)

	//处理汇报事件
	go func() {
		for {
			task := <-c.reportChan
			// log.Printf("%v", task)
			taskID := task.TaskID
			switch task.TaskType {
			case Map:
				if !c.mapTask[taskID].completed {
					c.mapTask[taskID].completed = true
					c.completedMapTaskNum += 1
				}

			case Reduce:
				if !c.reduceTask[taskID].completed {
					c.reduceTask[taskID].completed = true
					c.completedReduceTaskNum += 1
				}
			}
		}
	}()
	c.server()
	return &c
}
