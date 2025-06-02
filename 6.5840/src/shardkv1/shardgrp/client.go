package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
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

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.clientid = kvtest.RandValue(8)
	ck.opid = 0

	return ck
}

func (ck *Clerk) changeLeader() {
	// You will have to modify this function.
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
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

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientID: ck.clientid, OpID: ck.opid}
	reply := rpc.PutReply{}
	ck.opid += 1

	valid := false
	resend := false
	for !valid {
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
		return rpc.ErrMaybe
	}

	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
