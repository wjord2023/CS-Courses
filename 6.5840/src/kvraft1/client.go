package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader   int
	clientid string
	opid     int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, leader: 0}
	ck.clientid = kvtest.RandValue(8)
	ck.opid = 0
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	valid := false
	for !valid {
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrShutDown {
			ck.changeLeader()
		}
		valid = ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey)

		if !valid {
			time.Sleep(10 * time.Millisecond)
		}
		// DPrintf("Get %s from %d, reply: %v", key, ck.leader, reply)
	}

	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) changeLeader() {
	// You will have to modify this function.
	DPrintf("client", "change leader from %d to %d", ck.leader, (ck.leader+1)%len(ck.servers))
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientID: ck.clientid, OpID: ck.opid}
	reply := rpc.PutReply{}
	ck.opid += 1

	valid := false
	resend := false
	for !valid {
		DPrintf("client", "start to put value %s, version %d to %d", value, version, ck.leader)
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)

		if !ok {
			resend = true
		}

		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrShutDown {
			ck.changeLeader()
		}

		valid = ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrVersion || reply.Err == rpc.ErrNoKey)

		if !valid {
			time.Sleep(10 * time.Millisecond)
		}
	}

	if resend && reply.Err == rpc.ErrVersion {
		DPrintf("client", "put key %s %s, version %d, reply Err.Maybe", key, value, version)
		return rpc.ErrMaybe
	}

	DPrintf("client", "put key %s %s, version %d, reply %v", key, value, version, reply.Err)
	return reply.Err
}
