package client

import (
	// "bufio"
	"gfs"
	"gfs/util"

	// "sync"
	"time"

	sync "github.com/sasha-s/go-deadlock"
)

type leaseBuffer struct {
	sync.RWMutex
	master gfs.ServerAddress
	buffer map[gfs.ChunkHandle]*gfs.Lease
	tick   time.Duration
}

func newLeaseBuffer(ms gfs.ServerAddress, tick time.Duration) *leaseBuffer {
	ret := &leaseBuffer{
		master: ms,
		buffer: make(map[gfs.ChunkHandle]*gfs.Lease),
		tick:   tick,
	}

	//regular stale check
	go func() {
		ticker := time.Tick(tick) //ticker is a channel
		for now := range ticker {
			ret.Lock()
			for id, lease := range ret.buffer {
				if lease.Expire.Before(now) {
					delete(ret.buffer, id)
				}
			}
			ret.Unlock()
		}
	}()

	return ret
}

func (lm *leaseBuffer) GetLease(handle gfs.ChunkHandle) (*gfs.Lease, error) {
	lm.Lock()
	defer lm.Unlock()
	lease, ok := lm.buffer[handle]

	if !ok {
		var rep gfs.GetPrimaryAndSecondariesReply
		err := util.Call(lm.master, "Master.RPCGetPrimaryAndSecondaries", gfs.GetPrimaryAndSecondariesArg{Handle: handle}, &rep)
		if err != nil {
			return nil, err
		}
		lease = &gfs.Lease{
			Primary:     rep.Primary,
			Expire:      rep.Expire,
			Secondaries: rep.Secondaries,
		}
		lm.buffer[handle] = lease
		return lease, nil
	}
	return lease, nil
}
