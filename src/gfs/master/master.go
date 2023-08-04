package master

import (
	// "fmt"
	"encoding/gob"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path"
	"time"

	log "github.com/sirupsen/logrus"

	"gfs"
	"gfs/util"
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

// ------------ persistence ------------
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
	log.Info("[master]{loadMetaData}enter function: load metadata from ", filePath) 
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
	log.Info("[master]{loadMetaData} namespace: ", metadata.SeNamespace)
	return nil
}

func (m *Master) storeMetaData() error {
	filePath := path.Join(m.serverRoot, MetaDataFileName)
	log.Info("[master]{storeMetaData}enter function: store metadata to ", filePath)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, filePerm)
	if err != nil {
		log.Warning("[master]{storeMetaData} error: ", err)
		return err
	}
	defer file.Close()

	var metadata PersistentMetaData

	metadata.SeNamespace = m.nm.Serialize()

	log.Info("[master]{storeMetaData} namespace: ", metadata.SeNamespace)
	metadata.SeChunkInfo = m.cm.Serialize()

	log.Info("[master]{storeMetaData} chunkinfo: ", metadata.SeChunkInfo)

	err = gob.NewEncoder(file).Encode(metadata)
	if err != nil {
		log.Warning("[master]{storeMetaData} error: ", err)
	}
	return err
}

func (m *Master) initMetaData() {
	m.nm = newNamespaceManager()
	m.cm = newChunkManager()
	m.csm = newChunkServerManager()
	log.Info("[master]{initMetaData} init namespace and chunkmanager")
	m.loadMetaData()
}

// ------------ background ------------

func (m *Master) serverCheck() error {
	addr_list := m.csm.DetectDeadServers()

	for _, addr := range addr_list {
		log.Warning("[master]{serverCheck} remove server ", addr)
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
		log.Info("[master]{serverCheck} replica needed list: ", handles)
		m.cm.Lock()
		for i := 0; i < len(handles); i++ {
			ck := m.cm.chunks[handles[i]]
			if ck.expire.Before(time.Now()) {
				ck.Lock()
				err := m.reReplication(handles[i])
				log.Info("[master]{serverCheck} reReplication ", handles[i], err)
				ck.Unlock()
			}
		}
		m.cm.Unlock()
	}
	return nil
}

func (m *Master) reReplication(handle gfs.ChunkHandle) error {
	//make sure the chunk has been locked outside the function
	//lock the chunk outside the func will ensure that a new lease will not be granted during the reReplication
	from, to, err := m.csm.ChooseReReplication(handle)
	if err != nil {
		return err
	}
	log.Warningf("[master]{reReplication} allocate new chunk %v from %v to %v", handle, from, to)

	var rep gfs.CreateChunkReply
	err = util.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &rep)
	if err != nil {
		return err
	}
	var rep2 gfs.SendCopyReply
	err = util.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, &rep2)
	if err != nil {
		return err
	}
	m.cm.RegisterReplica(handle, to, false)
	m.csm.AddChunk([]gfs.ServerAddress{to}, handle) //since the required parameter is a slice, so we have to use []gfs.ServerAddress{to} instead of to
	return nil
}

// --------------interface----------------
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
				if !m.dead {
					log.Fatal("[master]{NewAndServe} master accept error:", err)
				}
				// log.Fatal("[master]{NewAndServer} master accept error:", err)
				// log.Exit(1)
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

	log.Infof("[master]{NewAndServer} Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	if !m.dead {
		log.Warning("[master]{Shutdown} Shutting down master at ", m.address)
		m.dead = true
		close(m.shutdown)
		m.l.Close()
	}

	log.Info("[master]{Shutdown} Storing metadata")
	err := m.storeMetaData()
	if err != nil {
		log.Warning("[master]{Shutdown} Error when storing metadata ", err)
	}
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	return nil
}

// ------------------RPC------------------
// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	log.Info("[master]{RPCHeartbeat} enter")
	isNew := m.csm.Heartbeat(args.Address)

	for _, handle := range args.LeaseExtensions {
		continue
		m.cm.ExtendLease(handle, args.Address)
	}

	if isNew { //if the chunkserver is new, then we need to let it report all the chunks it has
		var rep gfs.ReportSelfReply
		err := util.Call(args.Address, "ChunkServer.RPCReportSelf", gfs.ReportSelfArg{}, &rep)
		if err != nil {
			return err
		}

		for _, pci := range rep.Chunks {
			m.cm.RLock()
			version := m.cm.chunks[pci.Handle].version
			m.cm.RUnlock()

			if pci.Version == version {
				m.cm.RegisterReplica(pci.Handle, args.Address, true)
				m.csm.AddChunk([]gfs.ServerAddress{args.Address}, pci.Handle)
			} else {
				log.Warning("[master]{RPCHeartbeat} chunk ", pci.Handle, " version mismatch")
			}
		}
	}
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	lease, err := m.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		return err
	}
	reply.Primary = lease.Primary
	reply.Expire = lease.Expire
	reply.Secondaries = lease.Secondaries
	return nil
}

// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	servers, err := m.cm.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	reply.Locations = append(reply.Locations, servers...)
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, replay *gfs.CreateFileReply) error {
	args.Path = gfs.PathFormalizer(args.Path, false)
	err := m.nm.Create(args.Path)
	return err
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	args.Path = gfs.PathFormalizer(args.Path, true)
	err := m.nm.Mkdir(args.Path)
	return err
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	args.Path = gfs.PathFormalizer(args.Path, false)
	sp := args.Path.Path2SplitPath()
	filename := sp.Parts[len(sp.Parts)-1]
	cwd, err := m.nm.lockParents(sp, false)
	defer m.nm.unlockParents(sp)
	if err != nil {
		return err
	}
	file, ok := cwd.children[filename]
	if !ok {
		return fmt.Errorf("[master]{RPCGetFileInfo} error: %s does not exist", args.Path)
	}
	file.Lock()
	defer file.Unlock()

	reply.IsDir = file.isDir
	reply.Length = file.length
	reply.Chunks = file.chunks
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	args.Path = gfs.PathFormalizer(args.Path, false)
	sp := args.Path.Path2SplitPath()
	filename := sp.Parts[len(sp.Parts)-1]
	cwd, err := m.nm.lockParents(sp, false)
	defer m.nm.unlockParents(sp)
	if err != nil {
		return err
	}

	file, ok := cwd.children[filename]
	if !ok {
		return fmt.Errorf("[master]{RPCGetChunkHandle} error: %s does not exist", args.Path)
	}
	file.Lock()
	defer file.Unlock()

	if int(args.Index) == int(file.chunks) { //if the index is the next chunk, then create a new chunk
		log.Info("[master]{RPCGetChunkHandle} create new chunk for file: ", args.Path)
		file.chunks++
		log.Info("[master]{RPCGetChunkHandle} current server list: ", m.csm.servers)
		addrList, err := m.csm.ChooseServers(gfs.MinimumNumReplicas)
		if err != nil {
			return err
		}
		var successList []gfs.ServerAddress
		reply.Handle, successList, err = m.cm.CreateChunk(args.Path, addrList)
		if err != nil {
			log.Warning("[master]{RPCGetChunkHandle} create chunk error ", err)
		}
		m.csm.AddChunk(successList, reply.Handle)
		if len(successList) < gfs.MinimumNumReplicas {
			m.cm.Lock()
			m.cm.replicaNeededList = append(m.cm.replicaNeededList, reply.Handle)
			m.cm.Unlock()
		}
	} else {
		reply.Handle, err = m.cm.GetChunk(args.Path, args.Index)
	}
	return err
}

func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	args.Path = gfs.PathFormalizer(args.Path, true)
	var err error
	reply.Files, err = m.nm.List(args.Path)
	log.Info("[master]{RPCList} dir path: ", args.Path, "; files: ", reply.Files)
	return err
}
