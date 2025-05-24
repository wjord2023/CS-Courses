package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
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

	// Your definitions here.
	mu       sync.Mutex
	kvMap    map[Key]Value
	opRecord map[string][]int // map[clientID]opID
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

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
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

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
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

			DPrintf("server", "server %d store key %s : value %s, version %d", kv.me, args.Key, args.Value, 1)
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

		DPrintf("server", "server %d store key %s : value %s, version %d", kv.me, args.Key, args.Value, version+1)
		return &rpc.PutReply{Err: rpc.OK}
	} else {
		kv.mu.Unlock()
		return &rpc.PutReply{Err: rpc.ErrVersion}
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if rep != nil {
		*reply = *rep.(*rpc.GetReply)
		return
	}
	reply.Err = err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if rep != nil {
		*reply = *rep.(*rpc.PutReply)
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, mu: sync.Mutex{}, kvMap: make(map[Key]Value)}

	kv.opRecord = make(map[string][]int)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
