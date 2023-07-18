package master

import (
	// "fmt"
	"encoding/gob"
	"net"
	"net/rpc"
	"os"
	"path"
	"time"

	log "github.com/sirupsen/logrus"

	"gfs"
	// "gfs/util"
)

// Master Server struct
type Master struct {
	address    gfs.ServerAddress // master server address
	serverRoot string            // path to metadata storage
	l          net.Listener
	shutdown   chan struct{}
	dead       bool

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

const (
	MetaDataFileName = "master_meta"
	filePerm         = 0755
)

type PersistentMetaData struct {
	SeNamespace []serialTreeNode
	SeChunkInfo []serialChunkInfo
}

func (m *Master) loadMetaData() error {
	filePath := path.Join(m.serverRoot, MetaDataFileName)
	file, err := os.OpenFile(filePath, os.O_RDONLY, filePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metadata PersistentMetaData

	err = gob.NewDecoder(file).Decode(&metadata)
	if err != nil {
		return err
	}
	m.nm.Deserialize(metadata.SeNamespace)
	m.cm.Deserialize(metadata.SeChunkInfo)
	return nil
}

func (m *Master) storeMetaData() error {
	filePath := path.Join(m.serverRoot, MetaDataFileName)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, filePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metadata PersistentMetaData

	metadata.SeNamespace = m.nm.Serialize()
	metadata.SeChunkInfo = m.cm.Serialize()

	err = gob.NewEncoder(file).Encode(metadata)
	return err
}

func (m *Master) initMetaData() {
	m.nm = newNamespaceManager()
	m.cm = newChunkManager()
	m.csm = newChunkServerManager()
	m.loadMetaData()
	return
}

func (m *Master) serverCheck() error {
	addr_list := m.csm.DetectDeadServers()

	for _, addr := range addr_list {
		log.Warning("[master]{serverCheck} remove server %v", addr)
		handles, err := m.csm.RemoveServer(addr)
		if err != nil {
			return err
		}
		err = m.cm.RemoveChunk(handles, addr)
		if err != nil {
			return err
		}
	}

	handles := m.cm.GetReplicaNeededList()
	if handles != nil {
		log.Info("[master]{serverCheck} replica needed list: %v", handles)
		m.cm.Lock()
		for i := 0; i < len(handles); i++ {
			ck := m.cm.chunks[handles[i]]
			if ck.expire.Before(time.Now()) {
				ck.Lock()
				err := m.reReplication(handles[i])
				log.Info("[master]{serverCheck} reReplication %v", handles[i])
				log.Info("[master]{serverCheck} reReplication ", err)
				ck.Unlock()
			}
		}
		m.cm.Unlock()
	}
	return nil
}

func (m *Master) reReplication(handle gfs.ChunkHandle) error {
	//make sure the chunk has been locked outside the function
	from, to, err := m.csm.ChooseReReplication(handle)
	if err != nil {
		return err
	}
	
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address gfs.ServerAddress, serverRoot string) *Master {
	m := &Master{
		address:    address,
		serverRoot: serverRoot,
		shutdown:   make(chan struct{}),
	}

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	m.l = l

	// Initialize metadata
	m.initMetaData()

	// RPC Handler
	go func() {
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				log.Fatal("accept error:", err)
				log.Exit(1)
			}
		}
	}()

	// Background Task
	go func() {
		checkTicker := time.Tick(gfs.ServerCheckInterval)
		storeTicker := time.Tick(gfs.CheckPointInterval)

		for {
			var err error
			select {
			case <-m.shutdown:
				return
			case <-checkTicker:
				err = m.serverCheck()
			case <-storeTicker:
				err = m.storeMetaData()
			}

			if err != nil {
				log.Warning("Background error ", err)
			}
		}
	}()

	log.Infof("Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	if !m.dead {
		m.dead = true
		close(m.shutdown)
		m.l.Close()
	}

	err := m.storeMetaData()
	if err != nil {
		log.Warning("Error when storing metadata ", err)
	}
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	return nil
}

// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, replay *gfs.CreateFileReply) error {
	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, replay *gfs.MkdirReply) error {
	return nil
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	return nil
}

func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	return nil
}
