package shardgrp

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Key string

type Value struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu       sync.Mutex
	kvMap    map[Key]Value
	opRecord map[string][]int // map[clientID]opID
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(&req)
	case rpc.PutArgs:
		return kv.doPut(&req)
	default:
		log.Fatalf("unknown type %T", req)
	}
	return nil
}

func (kv *KVServer) appendOpRecord(clientID string, opID int) {
	if _, ok := kv.opRecord[clientID]; !ok {
		kv.opRecord[clientID] = make([]int, 0)
	}
	kv.opRecord[clientID] = append(kv.opRecord[clientID], opID)
}

func (kv *KVServer) checkOpRecord(clientID string, opID int) bool {
	opids, ok := kv.opRecord[clientID]
	if !ok {
		return false
	}
	for i := len(opids) - 1; i >= 0; i-- {
		if opids[i] == opID {
			return true
		}
	}
	return false
}

func (kv *KVServer) doGet(args *rpc.GetArgs) *rpc.GetReply {
	k := (Key)(args.Key)

	kv.mu.Lock()
	v, ok := kv.kvMap[k]
	kv.mu.Unlock()

	if !ok {
		return &rpc.GetReply{Err: rpc.ErrNoKey}
	}
	return &rpc.GetReply{Value: v.Value, Version: v.Version, Err: rpc.OK}
}

func (kv *KVServer) doPut(args *rpc.PutArgs) *rpc.PutReply {
	k := (Key)(args.Key)

	kv.mu.Lock()
	if kv.checkOpRecord(args.ClientID, args.OpID) {
		kv.mu.Unlock()
		return &rpc.PutReply{Err: rpc.OK}
	}

	v, ok := kv.kvMap[k]

	if !ok {
		if args.Version == 0 {
			kv.kvMap[k] = Value{Value: args.Value, Version: 1}
			kv.appendOpRecord(args.ClientID, args.OpID)
			kv.mu.Unlock()

			return &rpc.PutReply{Err: rpc.OK}
		} else {
			kv.mu.Unlock()
			return &rpc.PutReply{Err: rpc.ErrNoKey}
		}
	}

	version := v.Version

	if version == args.Version {
		kv.kvMap[k] = Value{Value: args.Value, Version: version + 1}
		kv.appendOpRecord(args.ClientID, args.OpID)
		kv.mu.Unlock()

		return &rpc.PutReply{Err: rpc.OK}
	} else {
		kv.mu.Unlock()
		return &rpc.PutReply{Err: rpc.ErrVersion}
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
