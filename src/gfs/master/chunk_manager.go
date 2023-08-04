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

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex

	chunks map[gfs.ChunkHandle]*chunkInfo
	files  map[gfs.Path]*fileInfo

	replicaNeededList []gfs.ChunkHandle // chunks that need more replicas
	numChunkHandle    gfs.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex
	location []gfs.ServerAddress // set of replica locations
	primary  gfs.ServerAddress   // primary chunkserver
	expire   time.Time           // lease expire time
	version  gfs.ChunkVersion    // for detecting stale chunk replicas
	checksum gfs.Checksum        // checksum of the chunk, for file check
	path     gfs.Path
}

type fileInfo struct {
	sync.RWMutex
	handles []gfs.ChunkHandle
}

type serialChunkInfo struct {
	Path gfs.Path
	Info []gfs.PersistentChunkInfo
}

func (cm *chunkManager) Serialize() []serialChunkInfo {
	cm.RLock()
	defer cm.RUnlock()

	var ret []serialChunkInfo
	for path, fi := range cm.files {
		var pcis []gfs.PersistentChunkInfo
		for _, handle := range fi.handles {
			pcis = append(pcis, gfs.PersistentChunkInfo{
				Handle:   handle,
				Length:   0,
				Version:  cm.chunks[handle].version,
				Checksum: 0, //todo
			})
		}
		ret = append(ret, serialChunkInfo{Path: path, Info: pcis})
	}
	return ret
}

func (cm *chunkManager) Deserialize(scis []serialChunkInfo) error {
	cm.Lock()
	defer cm.Unlock()

	present := time.Now()
	for _, sci := range scis {
		log.Info("[chunk_manager]{Deserialize} master restores file: ", sci.Path)
		fi := new(fileInfo)
		for _, pci := range sci.Info {
			fi.handles = append(fi.handles, pci.Handle)
			cm.chunks[pci.Handle] = &chunkInfo{
				expire:   present,
				version:  pci.Version,
				checksum: pci.Checksum,
			}
		}
		cm.numChunkHandle += gfs.ChunkHandle(len(sci.Info))
		cm.files[sci.Path] = fi
	}
	return nil
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunks: make(map[gfs.ChunkHandle]*chunkInfo),
		files:  make(map[gfs.Path]*fileInfo),
	}
	log.Info("################# new chunk manager #################")
	return cm
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress, useLock bool) error { // isLock is to show whether the chunk is locked
	var ci *chunkInfo
	var ok bool
	if useLock {
		cm.RLock()
		ci, ok = cm.chunks[handle]
		cm.RUnlock()

		ci.Lock()
		defer ci.Unlock()
	} else {
		ci, ok = cm.chunks[handle]
	}
	if !ok {
		return fmt.Errorf("[chunk_manager]{RegisterReplica} cannot find chunk %v", handle)
	}
	ci.location = append(ci.location, addr)
	return nil
} //diff

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) ([]gfs.ServerAddress, error) {
	cm.RLock()
	ci, ok := cm.chunks[handle]
	cm.RUnlock()
	if !ok {
		return nil, fmt.Errorf("[chunk_manager]{GetReplicas} cannot find chunk %v", handle)
	}
	return ci.location, nil
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	cm.RLock()
	fi, ok := cm.files[path]
	cm.RUnlock()
	if !ok {
		return -1, fmt.Errorf("[chunk_manager]{GetChunk} cannot find file %v[%v]", path, index)
	}
	if index < 0 || int(index) >= len(fi.handles) {
		return -1, fmt.Errorf("[chunk_manager]{GetChunk} index out of range %v[%v]", path, index)
	}
	return fi.handles[index], nil
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*gfs.Lease, error) {
	cm.RLock()
	ci, ok := cm.chunks[handle]
	cm.RUnlock()
	if !ok {
		return nil, fmt.Errorf("[chunk_manager]{GetLeaseHolder} cannot find chunk %v", handle)
	}
	ci.RLock()
	defer ci.RUnlock()

	ret := &gfs.Lease{}
	if ci.expire.Before(time.Now()) { // no one has a lease, grant one to a replica
		//check virsion first
		ci.version++
		rpc_arg := gfs.CheckVersionArg{Handle: handle, Version: ci.version}

		var wg sync.WaitGroup
		var freshList []gfs.ServerAddress
		var lock sync.Mutex //for freshList
		for _, addr := range ci.location {
			wg.Add(1)
			go func(addr gfs.ServerAddress) {
				defer wg.Done()
				var rpc_reply gfs.CheckVersionReply
				err := util.Call(addr, "ChunkServer.RPCCheckVersion", rpc_arg, &rpc_reply)
				if err == nil && !rpc_reply.Stale {
					lock.Lock()
					freshList = append(freshList, addr)
					lock.Unlock()
				} else {
					log.Warningf("[chunk_manager]{GetLeaseHolder} chunk %v staled", handle)
				}
			}(addr)
		}
		wg.Wait()

		if len(freshList) == 0 {
			log.Error("[chunk_manager]{GetLeaseHolder} no replica available")
			return nil, fmt.Errorf("[chunk_manager]{GetLeaseHolder} no replica available")
		}
		ci.location = freshList
		ci.primary = freshList[0]
		ci.expire = time.Now().Add(gfs.LeaseExpire)
	}
	ret.Primary = ci.primary
	ret.Expire = ci.expire
	for _, addr := range ci.location {
		if addr != ci.primary {
			ret.Secondaries = append(ret.Secondaries, addr)
		}
	}
	return ret, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	cm.Lock()
	defer cm.Unlock()
	ci, ok := cm.chunks[handle]
	if !ok {
		return fmt.Errorf("[chunk_manager]{ExtendLease} cannot find chunk %v", handle)
	}
	present := time.Now()
	if ci.primary != primary && ci.expire.After(present) {
		return fmt.Errorf("[chunk_manager]{ExtendLease} %v is not primary of chunk %v", primary, handle)
	}
	ci.primary = primary
	ci.expire = present.Add(gfs.LeaseExpire)
	return nil
}

// CreateChunk creates a new chunk for path.
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, []gfs.ServerAddress, error) {
	cm.Lock()
	defer cm.Unlock()

	handle := cm.numChunkHandle
	cm.numChunkHandle++

	fi, ok := cm.files[path]
	if !ok {
		fi = new(fileInfo)
		cm.files[path] = fi
	}
	fi.handles = append(fi.handles, handle)

	//update chunk info
	ci := &chunkInfo{path: path}
	cm.chunks[handle] = ci

	//call chunkserver to create chunk
	var errorInfo string
	var successList []gfs.ServerAddress
	for _, addr := range addrs {
		var rpc_reply gfs.CreateChunkReply
		err := util.Call(addr, "ChunkServer.RPCCreateChunk", handle, &rpc_reply)
		if err != nil {
			errorInfo += fmt.Sprintf("%v: %v\n", addr, err)
		} else {
			ci.location = append(ci.location, addr)
			successList = append(successList, addr)
		}
	}
	if errorInfo == "" {
		return handle, successList, nil
	} else {
		return handle, successList, fmt.Errorf("[chunk_manager]{CreateChunk} %v", errorInfo)
	}
}

// RemoveChunk removes chunks from a chunk manager
func (cm *chunkManager) RemoveChunk(handles []gfs.ChunkHandle, server gfs.ServerAddress) error {
	errList := ""
	for _, handle := range handles {
		cm.RLock()
		ci := cm.chunks[handle]
		cm.RUnlock()

		ci.Lock()
		for i, addr := range ci.location {
			if addr == server {
				ci.location = append(ci.location[:i], ci.location[i+1:]...) //remove the server from the location list
			}
		}
		ci.expire = time.Now()
		num := len(ci.location)
		ci.Unlock()

		if num < gfs.MinimumNumReplicas {
			cm.replicaNeededList = append(cm.replicaNeededList, handle)
			if num == 0 {
				errList += fmt.Sprintf("[chunk_manager]{RemoveChunk} chunk %v has no replica\n", handle)
			}
		}
	}
	if errList == "" {
		return nil
	} else {
		return fmt.Errorf(errList)
	}
}

// GetReplicaNeededList returns the list of chunks that need more replicas
func (cm *chunkManager) GetReplicaNeededList() []gfs.ChunkHandle {
	cm.Lock()
	defer cm.Unlock()

	if len(cm.replicaNeededList) == 0 {
		return nil
	} else {
		return cm.replicaNeededList
	}
}
