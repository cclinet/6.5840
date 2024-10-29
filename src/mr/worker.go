package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
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
	reducef func(string, []string) string) {
out:
	for {
		res := RequestNewTaskFunc()
		switch res.Type {
		case Map:
			HandleMap(mapf, res.Map.FileName, res.Map.ReduceTaskNum, res.Map.TaskID)
			ReportTaskCompleteFunc(Map, res.Map.TaskID)
		case Reduce:
			HandleReduce(reducef, res.Reduce.TaskID, res.Reduce.MapTaskNum)
			ReportTaskCompleteFunc(Reduce, res.Reduce.TaskID)
		case Continue:
			continue out
		case Stop:
			break out
		}
	}
	// ReportTaskCompleteFunc(1, 1)
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func RequestNewTaskFunc() RPCResponseArgs {
	req := RequestArgs{
		Type: RequestNewTask,
	}
	res := RPCResponseArgs{}
	ok := call("Coordinator.Call", &req, &res)
	if !ok {
		log.Fatal("call failed!\n")
	}
	return res
}

func ReportTaskCompleteFunc(TaskType int, TaskID int) {
	req := RequestArgs{
		Type: ReportTaskComplete,
		ReportTaskCompleteArgs: ReportTaskCompleteArgs{
			TaskType: TaskType,
			TaskID:   TaskID,
		},
	}
	res := RPCResponseArgs{}
	ok := call("Coordinator.Call", &req, &res)
	if !ok {
		log.Fatal("call failed!\n")
	}
}

func ReadFile(filename string) []byte {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}
	return content
}

func WriteKVFile(filename string, result []KeyValue) {
	f, err := os.CreateTemp("", filename)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(f.Name())

	enc := json.NewEncoder(f)
	for _, kv := range result {
		enc.Encode(kv)
	}
	os.Rename(f.Name(), filename)
}

func ReadKVFile(filename string) []KeyValue {
	var kvs []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	dec := json.NewDecoder(file)
	for dec.More() {
		var kv KeyValue
		err := dec.Decode(&kv)
		if err != nil {
			log.Fatal(err)
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func HandleMap(mapf func(string, string) []KeyValue, filename string, reduceTaskNum int, taskID int) {
	content := string(ReadFile(filename))
	result := mapf(filename, content)
	kvs := make([][]KeyValue, reduceTaskNum)
	for _, kv := range result {
		file_partition := ihash(kv.Key) % reduceTaskNum
		kvs[file_partition] = append(kvs[file_partition], kv)
	}
	for i, kv := range kvs {
		filename = fmt.Sprintf("mr-%v-%v", taskID, i)
		WriteKVFile(filename, kv)
	}
}

func HandleReduce(reducef func(string, []string) string, taskID int, mapTaskNum int) {
	var filenames []string
	oname := fmt.Sprintf("mr-out-%v", taskID)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	for i := range mapTaskNum {
		filenames = append(filenames, fmt.Sprintf("mr-%v-%v", i, taskID))
	}
	var kvs []KeyValue
	for _, filename := range filenames {
		kv := ReadKVFile(filename)
		kvs = append(kvs, kv...)
	}
	sort.Sort(ByKey(kvs))
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
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
