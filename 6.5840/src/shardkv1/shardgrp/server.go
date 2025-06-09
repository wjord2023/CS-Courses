package shardgrp

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/shardkv1/shardutils"
	tester "6.5840/tester1"
)

type Value struct {
	Value   string
	Version rpc.Tversion
}

type kvMap = map[string]Value

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu       sync.Mutex
	shards   map[shardcfg.Tshid]kvMap // map[shardID]map[Key]Value
	freezed  map[shardcfg.Tshid]bool  // whether the server is freezed
	opRecord map[string][]int         // map[clientID]opID

	freezeMaxNums  map[shardcfg.Tshid]shardcfg.Tnum
	installMaxNums map[shardcfg.Tshid]shardcfg.Tnum // max num of freeze/install/delete shard
	deleteMaxNums  map[shardcfg.Tshid]shardcfg.Tnum // max num of delete shard
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

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(&req)
	case rpc.PutArgs:
		return kv.doPut(&req)
	case shardrpc.FreezeShardArgs:
		return kv.doFreezeShard(&req)
	case shardrpc.InstallShardArgs:
		return kv.doInstallShard(&req)
	case shardrpc.DeleteShardArgs:
		return kv.doDeleteShard(&req)
	default:
		log.Fatalf("unknown type %T", req)
	}
	return nil
}

func (kv *KVServer) getShardMap(shard shardcfg.Tshid) kvMap {
	kvmap, ok := kv.shards[shard]
	if !ok {
		kvmap = make(kvMap)
		kv.shards[shard] = kvmap
	}
	return kvmap
}

func (kv *KVServer) doGet(args *rpc.GetArgs) *rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	k := args.Key
	shard := shardcfg.Key2Shard(k)

	kvmap := kv.getShardMap(shard)

	v, ok := kvmap[k]

	if !ok {
		if kv.freezed[shard] {
			return &rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		return &rpc.GetReply{Err: rpc.ErrNoKey}
	}
	return &rpc.GetReply{Value: v.Value, Version: v.Version, Err: rpc.OK}
}

func (kv *KVServer) doPut(args *rpc.PutArgs) *rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// shardutils.DPrintf("shardgrp", "KVServer %d: doPut %v", kv.me, args)
	k := args.Key
	shard := shardcfg.Key2Shard(k)
	if kv.freezed[shard] {
		return &rpc.PutReply{Err: rpc.ErrWrongGroup}
	}

	kvmap := kv.getShardMap(shard)

	if kv.checkOpRecord(args.ClientID, args.OpID) {
		return &rpc.PutReply{Err: rpc.OK}
	}

	v, ok := kvmap[k]

	if !ok {
		if args.Version == 0 {
			kvmap[k] = Value{Value: args.Value, Version: 1}
			kv.appendOpRecord(args.ClientID, args.OpID)

			return &rpc.PutReply{Err: rpc.OK}
		} else {
			return &rpc.PutReply{Err: rpc.ErrNoKey}
		}
	}

	version := v.Version

	if version == args.Version {
		kvmap[k] = Value{Value: args.Value, Version: version + 1}
		kv.appendOpRecord(args.ClientID, args.OpID)

		return &rpc.PutReply{Err: rpc.OK}
	} else {
		return &rpc.PutReply{Err: rpc.ErrVersion}
	}
}

func (kv *KVServer) doFreezeShard(args *shardrpc.FreezeShardArgs) *shardrpc.FreezeShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Num <= kv.freezeMaxNums[args.Shard] {
		return &shardrpc.FreezeShardReply{State: kvmapToBytes(kv.shards[args.Shard]), Num: kv.freezeMaxNums[args.Shard], Err: rpc.ErrWrongNum}
	}
	shardutils.DPrintf("shardgrp", "KVServer %d: doFreezeShard %v", kv.me, args)

	kv.freezeMaxNums[args.Shard] = args.Num
	kv.freezed[args.Shard] = true
	return &shardrpc.FreezeShardReply{State: kvmapToBytes(kv.shards[args.Shard]), Num: kv.freezeMaxNums[args.Shard], Err: rpc.OK}
}

func (kv *KVServer) doInstallShard(args *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Num <= kv.installMaxNums[args.Shard] {
		return &shardrpc.InstallShardReply{Err: rpc.ErrWrongNum}
	}
	shardutils.DPrintf("shardgrp", "KVServer %d: doInstallShard %v", kv.me, args)
	kv.installMaxNums[args.Shard] = args.Num
	kv.shards[args.Shard] = bytesToKvmap(args.State)
	kv.freezed[args.Shard] = false
	return &shardrpc.InstallShardReply{Err: rpc.OK}
}

func (kv *KVServer) doDeleteShard(args *shardrpc.DeleteShardArgs) *shardrpc.DeleteShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Num <= kv.deleteMaxNums[args.Shard] {
		return &shardrpc.DeleteShardReply{Err: rpc.ErrWrongNum}
	}
	shardutils.DPrintf("shardgrp", "KVServer %d: doDeleteShard %v", kv.me, args)
	kv.deleteMaxNums[args.Shard] = args.Num
	delete(kv.shards, args.Shard)
	// kv.freezed[args.Shard] = true
	return &shardrpc.DeleteShardReply{Err: rpc.OK}
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
	kv.mu.Lock()
	shard := shardcfg.Key2Shard(args.Key)
	if kv.freezed[shard] {
		reply.Err = rpc.ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	err, rep := kv.rsm.Submit(*args)
	if rep != nil {
		*reply = *rep.(*rpc.GetReply)
		return
	}
	reply.Err = err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	kv.mu.Lock()
	shard := shardcfg.Key2Shard(args.Key)
	if kv.freezed[shard] {
		reply.Err = rpc.ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	err, rep := kv.rsm.Submit(*args)
	if rep != nil {
		*reply = *rep.(*rpc.PutReply)
		return
	}
	reply.Err = err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if rep != nil {
		*reply = *rep.(*shardrpc.FreezeShardReply)
		return
	}
	reply.Err = err
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if rep != nil {
		*reply = *rep.(*shardrpc.InstallShardReply)
		return
	}
	reply.Err = err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if rep != nil {
		*reply = *rep.(*shardrpc.DeleteShardReply)
		return
	}
	reply.Err = err
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
	kv.shards = make(map[shardcfg.Tshid]kvMap)
	kv.freezed = make(map[shardcfg.Tshid]bool)
	kv.opRecord = make(map[string][]int)
	kv.freezeMaxNums = make(map[shardcfg.Tshid]shardcfg.Tnum)
	kv.installMaxNums = make(map[shardcfg.Tshid]shardcfg.Tnum)
	kv.deleteMaxNums = make(map[shardcfg.Tshid]shardcfg.Tnum)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
