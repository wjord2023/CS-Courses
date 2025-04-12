package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here

	lockKey string
	id      string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here

	lk.lockKey = l
	lk.id = kvtest.RandValue(8)

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.lockKey, lk.id, 0)
			if err == rpc.OK {
				return
			}
		}

		if value == "" {
			err = lk.ck.Put(lk.lockKey, lk.id, version)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrMaybe {
				lk.Release()
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	value, version, err := lk.ck.Get(lk.lockKey)
	if err == rpc.ErrNoKey {
		return
	}

	if value != lk.id {
		return
	}

	var puterr rpc.Err
	for puterr != rpc.OK && puterr != rpc.ErrMaybe {
		puterr = lk.ck.Put(lk.lockKey, "", version)
	}
}
