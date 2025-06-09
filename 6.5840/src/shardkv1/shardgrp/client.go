package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/shardkv1/shardutils"
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

func ShardgrpClerk(cfg *shardcfg.ShardConfig, clnt *tester.Clnt, shard shardcfg.Tshid) *Clerk {
	// Returns a clerk for the shard group responsible for the shard.
	_, group, ok := cfg.GidServers(shard)
	if !ok {
		panic("ShardgrpClerk: shard not found in config")
	}
	return MakeClerk(clnt, group)
}

func (ck *Clerk) changeLeader() {
	// You will have to modify this function.
	// shardutils.DPrintf("shardgrp", "Change leader from %d to %d", ck.leader, (ck.leader+1)%len(ck.servers))
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	valid := false
	failedTimes := 0
	for !valid {
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrShutDown {
			ck.changeLeader()
			failedTimes += 1
		}
		if failedTimes > 20 {
			return "", 0, rpc.ErrWrongGroup
		}

		valid = ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup)

		if !valid {
			time.Sleep(10 * time.Millisecond)
		}
		shardutils.DPrintf("shardgrp", "Get %s from %d, reply: %v", key, ck.leader, reply)
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
	failedTimes := 0
	for !valid {
		shardutils.DPrintf("shardgrp", "Put %s %s %d", key, value, version)
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)

		if !ok {
			resend = true
		}

		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrShutDown {
			ck.changeLeader()
			failedTimes += 1
		}
		if failedTimes > 20 {
			return rpc.ErrWrongGroup
		}

		valid = ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrVersion || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup)

		if !valid {
			time.Sleep(10 * time.Millisecond)
		}
		// shardutils.DPrintf("shardgrp", "Put %s %s %d, reply: %v", key, value, version, reply)
	}

	if resend && reply.Err == rpc.ErrVersion {
		return rpc.ErrMaybe
	}

	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}

	for valid := false; !valid; {
		shardutils.DPrintf("shardgrp", "FreezeShard %v %d", s, num)
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrShutDown {
			ck.changeLeader()
			time.Sleep(10 * time.Millisecond)
		}
		valid = ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrWrongNum)
	}
	// shardutils.DPrintf("shardgrp", "FreezeShard %v %d, state: %v, err: %v", s, num, reply.State, reply.Err)
	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{}

	for valid := false; !valid; {
		shardutils.DPrintf("shardgrp", "InstallShard %v %v %d", s, state, num)
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrShutDown {
			ck.changeLeader()
			time.Sleep(10 * time.Millisecond)
		}
		valid = ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrWrongNum)
	}
	// shardutils.DPrintf("shardgrp", "InstallShard %v %d, err: %v", s, num, reply.Err)
	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}

	for valid := false; !valid; {
		shardutils.DPrintf("shardgrp", "DeleteShard %v %d", s, num)
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrShutDown {
			ck.changeLeader()
			time.Sleep(10 * time.Millisecond)
		}
		valid = ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrWrongNum)
	}
	// shardutils.DPrintf("shardgrp", "DeleteShard %v %d, err: %v", s, num, reply.Err)
	return reply.Err
}
