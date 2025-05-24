package rsm

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  string
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	chans  map[string]chan any
	rsmid  string
	opid   int
	killed int32
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		chans:        make(map[string]chan any),
		rsmid:        RandID(8),
		opid:         0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.Reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	id := ID(rsm.rsmid, rsm.opid)
	op := Op{Me: rsm.me, Id: id, Req: req}
	_, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	ch := make(chan any)
	rsm.opid++

	rsm.chans[op.Id] = ch
	DPrintf("rsm %d Create chan for opid %v", rsm.me, op.Id)
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		close(ch)
		delete(rsm.chans, op.Id)
		DPrintf("rsm %d Delete chan for opid %v", rsm.me, op.Id)
		rsm.mu.Unlock()
	}()

	for {
		select {
		case ret := <-ch:
			DPrintf("rsm %d Get ret from chan for opid %v", rsm.me, op.Id)
			return rpc.OK, ret
		case <-time.After(100 * time.Millisecond):
			if rsm.isLeadershipChanged(term) {
				return rpc.ErrWrongLeader, nil
			}
			if rsm.Killed() {
				return rpc.ErrShutDown, nil
			}
		}
	}
}

func (rsm *RSM) isLeadershipChanged(term int) bool {
	newTerm, isLeader := rsm.rf.GetState()
	if !isLeader {
		return true
	}
	if newTerm != term {
		return true
	}
	return false
}

func (rsm *RSM) Reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			ret := rsm.sm.DoOp(op.Req)
			if op.Me != rsm.me {
				continue
			}

			rsm.mu.Lock()
			ch, ok := rsm.chans[op.Id]
			rsm.mu.Unlock()

			if ok {
				ch <- ret
				DPrintf("rsm %d Send ret to chan for opid %v", rsm.me, op.Id)
			}
		}
	}
	rsm.Kill()
}

func (rsm *RSM) Kill() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	rsm.killed = 1
}

func (rsm *RSM) Killed() bool {
	rsm.mu.Lock()
	ret := rsm.killed == 1
	rsm.mu.Unlock()
	return ret
}

func RandID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func ID(rsmid string, opid int) string {
	return rsmid + "-" + strconv.Itoa(opid)
}
