package chunkserver

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path"

	// "sync"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	log "github.com/sirupsen/logrus"

	"gfs"
	"gfs/util"
)

// ChunkServer struct
type ChunkServer struct {
	sync.RWMutex
	address    gfs.ServerAddress // chunkserver address
	master     gfs.ServerAddress // master address
	serverRoot string            // path to data storage
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to true if server is shutdown

	dl                     *downloadBuffer                // expiring download buffer
	pendingLeaseExtensions *util.ArraySet                 // pending lease extension
	chunk                  map[gfs.ChunkHandle]*chunkInfo // chunk information

	// for snapshot
	mutationResist bool
}

type Mutation struct {
	mtype   gfs.MutationType
	version gfs.ChunkVersion
	data    []byte
	offset  gfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	length   gfs.Offset
	version  gfs.ChunkVersion // version number of the chunk in disk
	checksum gfs.Checksum     // checksum of the chunk in disk
	// newestVersion gfs.ChunkVersion               // allocated newest version number
	mutations map[gfs.ChunkVersion]*Mutation // mutation buffer
	abandoned bool
}

const (
	MetaDataFileName = "chunkServer_meta"
	FilePerm         = 0755
	ChunkFilePerm    = 0644
)

// ----------- persistency -------------
func (cs *ChunkServer) loadMetaData() error {
	cs.Lock()
	defer cs.Unlock()

	filePath := path.Join(cs.serverRoot, MetaDataFileName)
	file, err := os.OpenFile(filePath, os.O_RDONLY, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var pcis []gfs.PersistentChunkInfo
	err = gob.NewDecoder(file).Decode(&pcis)
	if err != nil {
		return err
	}

	// log.Infof("[chunkserver]{loadMetaData} chunkserver: ", cs.address, " load metadata len: ", len(pcis))

	for _, pci := range pcis {
		cs.chunk[pci.Handle] = &chunkInfo{
			length:  pci.Length,
			version: pci.Version,
		}
	}
	return nil
}

func (cs *ChunkServer) storeMetaData() error {
	cs.RLock()
	defer cs.RUnlock()

	filePath := path.Join(cs.serverRoot, MetaDataFileName)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var pcis []gfs.PersistentChunkInfo
	for handle, ci := range cs.chunk {
		pcis = append(pcis, gfs.PersistentChunkInfo{
			Handle:  handle,
			Length:  ci.length,
			Version: ci.version,
		})
	}

	// log.Info("[chunkserver]{storeMetaData} server: ", cs.address, " store metadata of len: ", len(pcis))
	err = gob.NewEncoder(file).Encode(pcis)
	return err
}

// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, serverRoot string) *ChunkServer {
	cs := &ChunkServer{
		address:                addr,
		shutdown:               make(chan struct{}),
		master:                 masterAddr,
		serverRoot:             serverRoot,
		dl:                     newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet),
		chunk:                  make(map[gfs.ChunkHandle]*chunkInfo),
		mutationResist:         false, // for snapshot
	}
	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	cs.l = l

	//init root file
	_, err := os.Stat(serverRoot)
	if err != nil {
		err := os.Mkdir(serverRoot, FilePerm)
		if err != nil {
			log.Fatal("[chunkserver]{newAndServer} error in make serverRoot file: ", err)
		}
	}

	err = cs.loadMetaData()
	if err != nil {
		log.Warning("[chunkserver]{newAndServer} error in loadMetaData: ", err)
	}

	// RPC Handler
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, err := cs.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				// if chunk server is dead, ignores connection error
				if !cs.dead {
					log.Fatal(err)
				}
			}
		}
	}()

	// Heartbeat
	go func() {
		heartbeatTicker := time.Tick(gfs.HeartbeatInterval)
		storeTicker := time.Tick(gfs.ServerStoreInterval)
		cs.heartbeat()
		for {
			var err error
			select {
			case <-cs.shutdown:
				return
			case <-heartbeatTicker:
				cs.heartbeat()
			case <-storeTicker:
				err = cs.storeMetaData()
			}
			if err != nil {
				log.Warning("[chunkserver]{NewAndServer} background err: ", err)
			}
		}
	}()

	// log.Infof("[chunkserver]{NewAndServer} ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)
	return cs
}

func (cs *ChunkServer) heartbeat() error {
	rawList := cs.pendingLeaseExtensions.GetAllAndClear()
	handleList := make([]gfs.ChunkHandle, len(rawList))

	// log.Info("[chunkserver]{heartbeat} server: ", cs.address, " heartbeat len: ", len(rawList))
	for index, value := range rawList {
		handleList[index] = value.(gfs.ChunkHandle)
	}
	args := &gfs.HeartbeatArg{
		Address:         cs.address,
		LeaseExtensions: handleList,
	}
	err := util.Call(cs.master, "Master.RPCHeartbeat", args, &gfs.HeartbeatReply{})
	return err
}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warningf("ChunkServer %v shuts down", cs.address)
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
	err := cs.storeMetaData()
	if err != nil {
		log.Warning("[chunkserver]{Shutdown} error in storeMetaData: ", err)
	}
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
// TODO: This should be replaced by a chain forwarding.

// new version: RPCForwardData is called by either another replica or a client who sends data to the current memory buffer.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	// log.Infof("[chunkserver]{RPCForwardData} server: %v; receive dataID: %v", cs.address, args.DataID)
	if _, ok := cs.dl.Get(args.DataID); ok {
		return fmt.Errorf("[chunkserver]{RPCForwardData} error: dataID %v already exist", args.DataID)
	}
	cs.dl.Set(args.DataID, args.Data)

	if len(args.AddrChain) > 0 {
		nextAddr := args.AddrChain[0]
		args.AddrChain = args.AddrChain[1:]
		err := util.Call(nextAddr, "ChunkServer.RPCForwardData", args, reply)
		return err
	}
	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	cs.Lock()
	defer cs.Unlock()
	if cs.mutationResist {
		log.Warning("[chunkserver]{RPCCreateChunk} server: ", cs.address, " is in mutation resist mode")
		reply.ErrorCode = gfs.MutationResist
		return fmt.Errorf("[chunkserver]{RPCCreateChunk} server: %v is in mutation resist mode", cs.address)
	}

	// log.Info("[chunkserver]{RPCCreateChunk} server: ", cs.address, " create chunk: ", args.Handle, " start")
	if _, ok := cs.chunk[args.Handle]; ok {
		log.Warning("[chunkserver]{RPCCreateChunk} server: ", cs.address, " create chunk: ", args.Handle, " already exist")
		return nil
	}
	cs.chunk[args.Handle] = &chunkInfo{
		length: 0,
	}
	filename := cs.getChunkFilePath(args.Handle)
	_, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, ChunkFilePerm)

	return err
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	handle := args.Handle
	cs.RLock()
	ci, ok := cs.chunk[handle]
	cs.RUnlock()
	if !ok || ci.abandoned {
		return fmt.Errorf("[chunkserver]{RPCReadChunk} error: chunk %v not found or abandoned", handle)
	}
	var err error
	reply.Data = make([]byte, args.Length)
	ci.RLock()
	reply.Length, err = cs.readChunk(handle, reply.Data, args.Offset)
	ci.RUnlock()
	if err == io.EOF {
		reply.ErrorCode = gfs.ReadEOF
		return nil
	}
	return err
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	// log.Info("[chunkserver]{RPCWriteChunk} server: ", cs.address, " write chunk: ", args.DataID, " start")
	cs.RLock()
	if cs.mutationResist {
		defer cs.RUnlock()
		log.Warning("[chunkserver]{RPCCreateChunk} server: ", cs.address, " is in mutation resist mode")
		reply.ErrorCode = gfs.MutationResist
		return fmt.Errorf("[chunkserver]{RPCWriteChunk} server: %v is in mutation resist mode", cs.address)
	}
	cs.RUnlock()

	data, err := cs.dl.GetAndDelete(args.DataID)
	if err != nil {
		return err
	}
	newLen := args.Offset + gfs.Offset(len(data))
	if newLen > gfs.MaxChunkSize {
		return fmt.Errorf("[chunkserver]{RPCWriteChunk} error: write exceed chunk size")
	}
	handle := args.DataID.Handle
	cs.RLock()
	ci, ok := cs.chunk[handle]
	cs.RUnlock()

	if !ok || ci.abandoned {
		return fmt.Errorf("[chunkserver]{RPCWriteChunk} error: chunk %v not found or abandoned", handle)
	}

	ci.Lock()
	mutations := &Mutation{
		mtype:  gfs.MutationWrite,
		data:   data,
		offset: args.Offset,
	}
	err = cs.applyMutation(handle, mutations)
	if err == nil {
		callArgs := gfs.ApplyMutationArg{
			Mtype:  gfs.MutationWrite,
			DataID: args.DataID,
			Offset: args.Offset,
		}
		err = util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", callArgs)
	}
	ci.Unlock()

	if err != nil {
		return err
	}
	cs.pendingLeaseExtensions.Add(handle)
	return nil
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	cs.RLock()
	if cs.mutationResist {
		log.Warning("[chunkserver]{RPCCreateChunk} server: ", cs.address, " is in mutation resist mode")
		reply.ErrorCode = gfs.MutationResist
		return fmt.Errorf("[chunkserver]{RPCAppendChunk} server: %v is in mutation resist mode", cs.address)
	}
	cs.RUnlock()

	data, err := cs.dl.GetAndDelete(args.DataID)
	if err != nil {
		return err
	}
	if len(data) > gfs.MaxAppendSize {
		return fmt.Errorf("[chunkserver]{RPCAppendChunk} error: append exceed max append size")
	}
	handle := args.DataID.Handle
	cs.RLock()
	ci, ok := cs.chunk[handle]
	cs.RUnlock()
	if !ok || ci.abandoned {
		return fmt.Errorf("[chunkserver]{RPCAppendChunk} error: chunk %v not found or abandoned", handle)
	}
	var mtype gfs.MutationType

	if err = func() error {
		ci.Lock()
		defer ci.Unlock()
		newLen := ci.length + gfs.Offset(len(data))
		offset := ci.length
		if newLen > gfs.MaxChunkSize {
			mtype = gfs.MutationPad
			ci.length = gfs.MaxChunkSize
			reply.ErrorCode = gfs.AppendExceedChunkSize
		} else {
			mtype = gfs.MutationAppend
			ci.length = newLen
		}
		reply.Offset = offset
		mutation := &Mutation{
			mtype:  mtype,
			data:   data,
			offset: offset,
		}
		wait := make(chan error, 1)
		go func() {
			wait <- cs.applyMutation(handle, mutation)
		}()
		callArgs := gfs.ApplyMutationArg{
			Mtype:  mtype,
			DataID: args.DataID,
			Offset: offset,
		}
		err = util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", callArgs)
		if err != nil {
			return err
		}
		err = <-wait
		return err
	}(); err != nil {
		return err
	}
	cs.pendingLeaseExtensions.Add(handle)
	return nil
}

// RPCApplyWriteChunk is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	data, err := cs.dl.GetAndDelete(args.DataID)
	if err != nil {
		return err
	}
	handle := args.DataID.Handle
	cs.RLock()
	ci, ok := cs.chunk[handle]
	cs.RUnlock()
	if !ok || ci.abandoned {
		return fmt.Errorf("[chunkserver]{RPCApplyMutation} error: chunk %v not found or abandoned", handle)
	}
	mutation := &Mutation{
		mtype:  args.Mtype,
		data:   data,
		offset: args.Offset,
	}
	err = func() error {
		ci.Lock()
		defer ci.Unlock()
		return cs.applyMutation(handle, mutation)
	}()
	return err
}

// RPCSendCCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	handle := args.Handle
	cs.RLock()
	ci, ok := cs.chunk[handle]
	cs.RUnlock()
	if !ok || ci.abandoned {
		return fmt.Errorf("[chunkserver]{RPCSendCopy} error: chunk %v not found or abandoned", handle)
	}
	ci.RLock()
	defer ci.RUnlock()

	data := make([]byte, ci.length)
	_, err := cs.readChunk(handle, data, 0)
	if err != nil {
		return err
	}

	err = util.Call(args.Address, "ChunkServer.RPCApplyCopy", gfs.ApplyCopyArg{Handle: handle, Data: data, Version: ci.version}, &gfs.ApplyCopyReply{})
	return err
}

// RPCSendCCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	handle := args.Handle
	cs.RLock()
	if cs.mutationResist {
		defer cs.RUnlock()
		log.Warning("[chunkserver]{RPCApplyCopy} server: ", cs.address, " is in mutation resist mode")
		reply.ErrorCode = gfs.MutationResist
		return fmt.Errorf("[chunkserver]{RPCApplyCopy} server: %v is in mutation resist mode", cs.address)
	}
	ci, ok := cs.chunk[handle]
	cs.RUnlock()
	if !ok || ci.abandoned {
		return fmt.Errorf("[chunkserver]{RPCApplyCopy} error: chunk %v not found or abandoned", handle)
	}
	ci.Lock()
	defer ci.Unlock()

	ci.version = args.Version
	err := cs.writeChunk(handle, args.Data, 0)
	return err
}

// RPCCheckVersion is called by master to check version and detect stale chunk
func (cs *ChunkServer) RPCCheckVersion(args gfs.CheckVersionArg, reply *gfs.CheckVersionReply) error {
	// log.Info("[chunkserver]{RPCCheckVersion} check version for chunk: ", args.Handle)
	cs.RLock()
	ci, ok := cs.chunk[args.Handle]
	cs.RUnlock()
	if !ok || ci.abandoned {
		return fmt.Errorf("[chunkserver]{RPCCheckVersion} error: chunk %v not found or abandoned", args.Handle)
	}
	ci.Lock()
	defer ci.Unlock()

	// log.Info("[chunkserver]{RPCCheckVersion} local version: ", ci.version, " master version: ", args.Version)

	if ci.version+gfs.ChunkVersion(1) == args.Version {
		ci.version++
		// log.Info("[chunkserver]{RPCCheckVersion} chunk ", args.Handle, " version updated to ", ci.version)
		reply.Stale = false
	} else {
		log.Warningf("[chunkserver]{RPCCheckVersion} chunk %v version mismatch, local version: %v, master version: %v", args.Handle, ci.version, args.Version)
		reply.Stale = true
		ci.abandoned = true
	}
	return nil
}

// ----------------added by me----------------
func (cs *ChunkServer) RPCReportSelf(args gfs.ReportSelfArg, reply *gfs.ReportSelfReply) error {
	cs.RLock()
	defer cs.RUnlock()

	// log.Info("[ChunkServer]{RPCReportSelf} server: ", cs.address, " report self")
	var ret []gfs.PersistentChunkInfo
	for handle, ci := range cs.chunk {
		ret = append(ret, gfs.PersistentChunkInfo{
			Handle:   handle,
			Length:   ci.length,
			Version:  ci.version,
			Checksum: ci.checksum,
		})
	}
	reply.Chunks = ret
	// log.Info("[chunkserver]{RPCReportSelf}", cs.address, ": self report end")
	return nil
}

// -------------- repeated function ----------------
func (cs *ChunkServer) getChunkFilePath(handle gfs.ChunkHandle) string {
	return path.Join(cs.serverRoot, fmt.Sprintf("chunk%v.chk", handle))
}

func (cs *ChunkServer) readChunk(handle gfs.ChunkHandle, data []byte, offset gfs.Offset) (int, error) {
	filename := cs.getChunkFilePath(handle)
	file, err := os.Open(filename)
	if err != nil {
		return -1, err
	}
	defer file.Close()
	return file.ReadAt(data, int64(offset))
}

func (cs *ChunkServer) writeChunk(handle gfs.ChunkHandle, data []byte, offset gfs.Offset) error {
	cs.RLock()
	ci := cs.chunk[handle]
	cs.RUnlock()

	newLength := offset + gfs.Offset(len(data))
	if newLength > ci.length {
		ci.length = newLength
	}

	filename := cs.getChunkFilePath(handle)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, ChunkFilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(offset))
	if err != nil {
		log.Warningf("[chunkserver]{writeChunk} error in write: ", err)
	}

	return err
}

func (cs *ChunkServer) applyMutation(handle gfs.ChunkHandle, mut *Mutation) error {
	var err error
	if mut.mtype == gfs.MutationPad {
		data := []byte{0}
		err = cs.writeChunk(handle, data, gfs.MaxChunkSize-1) //when you write to the end of the file, the previous data will be padded with 0
	} else {
		err = cs.writeChunk(handle, mut.data, mut.offset)
	}
	if err != nil {
		cs.RLock()
		ci := cs.chunk[handle]
		cs.RUnlock()
		log.Warningf("[chunkserver]{applyMutation} error in writeChunk: ", err)
		ci.abandoned = true
		return err
	}
	return nil
}

// for snapshot
func (cs *ChunkServer) RPCStartSnapshot(args gfs.StartSnapshotArg, reply *gfs.StartSnapshotReply) error {
	cs.Lock()
	cs.mutationResist = true
	cs.Unlock()
	return nil
}

func (cs *ChunkServer) RPCEndSnapshot(args gfs.EndSnapshotArg, reply *gfs.EndSnapshotReply) error {
	cs.Lock()
	cs.mutationResist = false
	cs.Unlock()
	return nil
}
