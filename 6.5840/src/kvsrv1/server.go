package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Key string

type Value struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvMap map[Key]Value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvMap = make(map[Key]Value)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	k := (Key)(args.Key)

	kv.mu.Lock()
	v, ok := kv.kvMap[k]
	kv.mu.Unlock()

	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = v.Value
	reply.Version = v.Version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	k := (Key)(args.Key)

	kv.mu.Lock()
	v, ok := kv.kvMap[k]

	if !ok {
		if args.Version == 0 {
			kv.kvMap[k] = Value{Value: args.Value, Version: 1}
			kv.mu.Unlock()

			reply.Err = rpc.OK
			return
		} else {
			kv.mu.Unlock()
			reply.Err = rpc.ErrNoKey
			return
		}
	}

	version := v.Version

	if version == args.Version {
		kv.kvMap[k] = Value{Value: args.Value, Version: version + 1}
		kv.mu.Unlock()

		reply.Err = rpc.OK
		return
	} else {
		kv.mu.Unlock()
		reply.Err = rpc.ErrVersion
		return
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
