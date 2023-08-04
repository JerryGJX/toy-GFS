package master

import (
	"fmt"
	"gfs"
	"gfs/util"

	// "gfs/util"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers: make(map[gfs.ServerAddress]*chunkServerInfo),
	}
	log.Infof("################ new chunkserver manager ################")
	return csm
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunkserver has
}

// Hearbeat marks the chunkserver alive for now.
func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress) bool {
	log.Info("[chunkserver manager]{Heartbeat} enter")
	csm.Lock()
	defer csm.Unlock()

	csi, ok := csm.servers[addr]
	if !ok {
		log.Info("[chunkserver manager]{HeartBeat} new chunkserver: ", addr)
		csm.servers[addr] = &chunkServerInfo{
			lastHeartbeat: time.Now(),
			chunks:        make(map[gfs.ChunkHandle]bool),
		}
		return true
	} else {
		//todo: add garbage collection
		csi.lastHeartbeat = time.Now()
		return false
	}
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()

	for _, addr := range addrs {
		csm.servers[addr].chunks[handle] = true
	}
}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle) (from, to gfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()

	from = ""
	to = ""
	err = nil
	for addr, csi := range csm.servers {
		if csi.chunks[handle] {
			from = addr
		} else {
			to = addr
		}
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("[chunckserver_manager]{ChooseReReplication} failed: no enough chunkservers")
	return
}

// ChooseServers returns servers to store new chunk.
// It is called when a new chunk is create
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {
	csm.RLock()
	defer csm.RUnlock()

	if len(csm.servers) < num {
		return nil, fmt.Errorf("[chunkserver_manager]{ChooseServers} failed: no enough chunkservers; %d < %d", len(csm.servers), num)
	}

	var all, ret []gfs.ServerAddress
	for addr := range csm.servers {
		all = append(all, addr)
	}
	choose, err := util.Sample(len(all), num)
	if err != nil {
		return nil, err
	}
	for _, addr := range choose {
		ret = append(ret, all[addr])
	}

	return ret, nil
}

// DetectDeadServers detects disconnected chunkservers according to last heartbeat time
func (csm *chunkServerManager) DetectDeadServers() []gfs.ServerAddress {
	csm.RLock()
	defer csm.RUnlock()

	var ret []gfs.ServerAddress
	present := time.Now()
	for addr, csi := range csm.servers {
		if csi.lastHeartbeat.Add(gfs.ServerTimeout).Before(present) {
			ret = append(ret, addr)
		}
	}
	return ret
}

// RemoveServers removes metedata of a disconnected chunkserver.
// It returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	csm.Lock()
	defer csm.Lock()

	err = nil
	csi, ok := csm.servers[addr]
	if !ok {
		err = fmt.Errorf("[chunkserver_manager]{RemoveServer} failed: no such chunkserver")
		return
	}
	for handle, flag := range csi.chunks {
		if flag {
			handles = append(handles, handle)
		}
	}
	delete(csm.servers, addr)
	return
}
