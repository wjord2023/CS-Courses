package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"reflect"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardutils"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	skipVersionCheck bool // skip version check for testing
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	shardutils.DPrintf("shardctrler", "MakeShardCtrler: %s", srv)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	shardutils.DPrintf("shardctrler", "InitController")
	curr := sck.Query()
	next := sck.QueryNext()
	if next.Num > curr.Num {
		shardutils.DPrintf("shardctrler", "InitController: ChangeConfig from %s to %s", curr.String(), next.String())
		sck.ChangeConfigTo(next)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	sck.Put("cfg", cfg.String(), 0)
	sck.Put("nxt", cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	// _, version, _ := sck.Get("nxt")
	old := sck.Query()
	if new.Num <= old.Num {
		return // Ignore if the new config is not newer than the current one
	}

	sck.Put("nxt", new.String(), rpc.Tversion(new.Num-1))

	nxt := sck.QueryNext()

	for i := range shardcfg.NShards {
		oldGid, oldSrvs, oldOk := old.GidServers(shardcfg.Tshid(i))
		newGid, newSrvs, newOk := nxt.GidServers(shardcfg.Tshid(i))
		if !oldOk || !newOk {
			panic("ChangeConfigTo: shard not found in config")
		}
		if oldGid != newGid || !reflect.DeepEqual(oldSrvs, newSrvs) {
			oldClerk := shardgrp.ShardgrpClerk(old, sck.clnt, shardcfg.Tshid(i))
			oldState, err := oldClerk.FreezeShard(shardcfg.Tshid(i), nxt.Num)
			if err == rpc.ErrWrongGroup {
				return // If freezing the shard fails, abort the change
			}
			newClerk := shardgrp.ShardgrpClerk(nxt, sck.clnt, shardcfg.Tshid(i))

			err = newClerk.InstallShard(shardcfg.Tshid(i), oldState, nxt.Num)
			if err == rpc.ErrWrongGroup {
				return
			}
			err = oldClerk.DeleteShard(shardcfg.Tshid(i), nxt.Num)
			if err == rpc.ErrWrongGroup {
				return
			}
		}
	}
	_, version, _ := sck.Get("cfg")
	sck.Put("cfg", nxt.String(), version)
	shardutils.DPrintf("shardctrler", "ChangeConfig completed, new config: %s", new.String())
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfgStr, _, _ := sck.Get("cfg")
	return shardcfg.FromString(cfgStr)
}

func (sck *ShardCtrler) QueryNext() *shardcfg.ShardConfig {
	// Your code here.
	cfgStr, _, _ := sck.Get("nxt")
	return shardcfg.FromString(cfgStr)
}
