package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Log struct {
	messageID int64
	result    string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data            map[string]string
	clientMessageID map[int64]Log
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {

	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.MessageID == kv.clientMessageID[args.ClientID].messageID {
		reply.Value = kv.clientMessageID[args.ClientID].result
		return
	}

	oldValue := kv.data[args.Key]
	kv.data[args.Key] = oldValue + args.Value
	kv.clientMessageID[args.ClientID] = Log{args.MessageID, oldValue}
	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.clientMessageID = make(map[int64]Log)

	// You may need initialization code here.

	return kv
}
