package master

import (
	"fmt"
	"gfs"
	// "gfs/util"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

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
	for path, fi := range cm.file {
		var pcis []gfs.PersistentChunkInfo
		for _, handle := range fi.handles {
			pcis = append(pcis, gfs.PersistentChunkInfo{
				Handle:   handle,
				Length:   0,
				Version:  cm.chunk[handle].version,
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
			cm.chunk[pci.Handle] = &chunkInfo{
				expire:   present,
				version:  pci.Version,
				checksum: pci.Checksum,
			}
		}
		cm.numChunkHandle += gfs.ChunkHandle(len(sci.Info))
		cm.file[sci.Path] = fi
	}
	return nil
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	log.Info("################# new chunk manager #################")
	return cm
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress, isLocked bool) error { // isLock is to show whether the chunk is locked
	var ci *chunkInfo
	var ok bool
	if isLocked {
		ci, ok = cm.chunk[handle]
	} else {
		cm.RLock()
		ci, ok = cm.chunk[handle]
		cm.RUnlock()

		ci.Lock()
		defer ci.Unlock()
	}
	if !ok {
		return fmt.Errorf("[chunk_manager]{RegisterReplica} cannot find chunk %v", handle)
	}
	ci.location = append(ci.location, addr)
	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) ([]gfs.ServerAddress, error) {
	cm.RLock()
	ci, ok := cm.chunk[handle]
	cm.RUnlock()
	if !ok {
		return nil, fmt.Errorf("[chunk_manager]{GetReplicas} cannot find chunk %v", handle)
	}
	return ci.location, nil
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	return 0, nil
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*gfs.Lease, error) {
	return nil, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	return nil
}

// CreateChunk creates a new chunk for path.
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, error) {
	return 0, nil
}
