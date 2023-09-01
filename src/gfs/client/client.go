package client

import (
	"fmt"
	"gfs"
	"gfs/chunkserver"
	"gfs/util"
	"io"
	"math/rand"
	"time"
	log "github.com/sirupsen/logrus"
)

// Client struct is the GFS client-side driver
type Client struct {
	master   gfs.ServerAddress
	leaseBuf *leaseBuffer
}

// NewClient returns a new gfs client.
func NewClient(master gfs.ServerAddress) *Client {
	return &Client{
		master:   master,
		leaseBuf: newLeaseBuffer(master, gfs.LeaseBufferTick),
	}
}

// Create creates a new file on the specific path on GFS.
func (c *Client) Create(path gfs.Path) error {
	// log.Info("[client]{Create} enter function, path: ", path)
	path = gfs.PathFormalizer(path, false)
	var rep gfs.CreateFileReply
	err := util.Call(c.master, "Master.RPCCreateFile", gfs.CreateFileArg{Path: path}, &rep)
	if err != nil {
		return err
	}
	return nil
}

// Mkdir creates a new directory on GFS.
func (c *Client) Mkdir(path gfs.Path) error {
	path = gfs.PathFormalizer(path, true)
	var rep gfs.MkdirReply
	err := util.Call(c.master, "Master.RPCMkdir", gfs.MkdirArg{Path: path}, &rep)
	if err != nil {
		return err
	}
	return nil
}

// List lists everything in specific directory on GFS.
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error) {
	path = gfs.PathFormalizer(path, true)
	var rep gfs.ListReply
	err := util.Call(c.master, "Master.RPCList", gfs.ListArg{Path: path}, &rep)
	if err != nil {
		return nil, err
	}
	return rep.Files, nil
}

// Read reads the file at specific offset.
// It reads up to len(data) bytes form the File.
// It return the number of bytes, and an error if any.
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (int, error) {
	path = gfs.PathFormalizer(path, false)
	var info gfs.GetFileInfoReply
	var err error
	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &info)
	if err != nil {
		return -1, err
	}

	if int64(offset/gfs.MaxChunkSize) > info.Chunks {
		return -1, fmt.Errorf("[Read] error: offset is out of range")
	}
	pos := 0
	for pos < len(data) {
		index := gfs.ChunkIndex(offset / gfs.MaxChunkSize)
		chunkOffset := offset % gfs.MaxChunkSize
		if int64(index) >= info.Chunks {
			err = gfs.Error{Code: gfs.ReadEOF, Err: "read EOF"}
			break
		}
		var handle gfs.ChunkHandle
		handle, err = c.GetChunkHandle(path, index)
		if err != nil {
			break
		}
		var n int

		for {
			n, err = c.ReadChunk(handle, chunkOffset, data[pos:])
			if err == nil || err.(gfs.Error).Code == gfs.ReadEOF {
				break
			}
		}

		offset += gfs.Offset(n)
		pos += n
		if err != nil {
			break
		}
	}
	if err != nil && err.(gfs.Error).Code == gfs.ReadEOF {
		return pos, io.EOF
	} else {
		return pos, err
	}
}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	path = gfs.PathFormalizer(path, false)
	var rep gfs.GetFileInfoReply
	err := util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &rep)
	if err != nil {
		return err
	}

	if int64(offset/gfs.MaxChunkSize) > rep.Chunks {
		return fmt.Errorf("[Write] error: offset is out of range")
	}

	startPoint := 0
	for {
		index := gfs.ChunkIndex(offset / gfs.MaxChunkSize)
		chunkOffset := offset % gfs.MaxChunkSize //the length of data mod by MaxChunkSize
		handle, err := c.GetChunkHandle(path, index)
		if err != nil {
			return err
		}
		maxLen := int(gfs.MaxChunkSize - chunkOffset)
		var writeLen int
		if startPoint+maxLen > len(data) {
			writeLen = len(data) - startPoint
		} else {
			writeLen = maxLen
		}

		for {
			err = c.WriteChunk(handle, chunkOffset, data[startPoint:startPoint+writeLen])
			if err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
		offset += gfs.Offset(writeLen)
		startPoint += writeLen
		if startPoint == len(data) {
			break
		}
	}
	return nil
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (gfs.Offset, error) {
	// log.Info("[client]{Append} enter function, path: ", path)
	path = gfs.PathFormalizer(path, false)
	var err error
	if len(data) > gfs.MaxChunkSize {
		return 0, fmt.Errorf("[client]{Append} error: data is too long")
	}
	var rep gfs.GetFileInfoReply

	// log.Info("[client]{Append} try to get file info")
	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &rep)
	if err != nil {
		return 0, err
	}

	startIndex := gfs.ChunkIndex(rep.Chunks - 1)
	if startIndex < 0 {
		startIndex = 0
	}

	var offsetInChunk gfs.Offset
	for {
		// log.Info("[client]{Append} try to get chunk handle")
		handle, err := c.GetChunkHandle(path, startIndex)
		// log.Info("[client]{Append} get chunk handle: ", handle)
		if err != nil {
			return 0, err
		}

		for {
			// log.Info("[client]{Append} try to append chunk")
			offsetInChunk, err = c.AppendChunk(handle, data)
			// log.Info("[client]{Append} append chunk with offset: ", offsetInChunk)
			if err == nil || err.(gfs.Error).Code == gfs.AppendExceedChunkSize {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if err == nil || err.(gfs.Error).Code != gfs.AppendExceedChunkSize {
			break
		}
		startIndex++
		// log.Info("[client]{Append} a Chunk is appended, try next")
	}

	if err != nil {
		return 0, err
	}

	realOffset := gfs.Offset(startIndex)*gfs.MaxChunkSize + offsetInChunk
	return realOffset, nil
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	// log.Info("[client]{GetChunkHandle} enter function, path: ", path, "; index: ", index)
	path = gfs.PathFormalizer(path, false)
	var rep gfs.GetChunkHandleReply
	err := util.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{Path: path, Index: index}, &rep)
	if err != nil {
		return 0, err
	}
	return rep.Handle, nil
}

// ReadChunk reads data from the chunk at specific offset.
// len(data)+offset  should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	var readLen int
	if offset+gfs.Offset(len(data)) > gfs.MaxChunkSize {
		readLen = int(gfs.MaxChunkSize - offset)
	} else {
		readLen = len(data)
	}
	var rep gfs.GetReplicasReply
	err := util.Call(c.master, "Master.RPCGetReplicas", gfs.GetReplicasArg{Handle: handle}, &rep)
	if err != nil {
		return 0, gfs.Error{Code: gfs.UnknownError, Err: err.Error()}
	}
	targetServer := rep.Locations[rand.Intn(len(rep.Locations))]

	var rep2 gfs.ReadChunkReply
	rep2.Data = data

	err = util.Call(targetServer, "ChunkServer.RPCReadChunk", gfs.ReadChunkArg{Handle: handle, Offset: offset, Length: readLen}, &rep2)
	if err != nil {
		return 0, gfs.Error{Code: gfs.UnknownError, Err: err.Error()}
	}
	if rep2.ErrorCode == gfs.ReadEOF {
		return rep2.Length, gfs.Error{Code: gfs.ReadEOF, Err: "read EOF"}
	}
	return rep2.Length, nil
}

// WriteChunk writes data to the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	// log.Infof("[client]{WriteChunk}enter function; handle: %v, offset: %v", handle, offset)
	if int(offset)+len(data) > gfs.MaxChunkSize {
		return fmt.Errorf("[client]{WriteChunk} error: data is too long for one chunk")
	}
	lease, err := c.leaseBuf.GetLease(handle)
	if err != nil {
		return err
	}
	dataID := chunkserver.NewDataID(handle)
	chain := append(lease.Secondaries, lease.Primary) //randomness will be granted by the master

	var rep gfs.ForwardDataReply
	err = util.Call(chain[0], "ChunkServer.RPCForwardData", gfs.ForwardDataArg{DataID: dataID, Data: data, AddrChain: chain[1:]}, &rep)
	if err != nil {
		return err
	}
	var rep2 gfs.WriteChunkReply
	err = util.Call(lease.Primary, "ChunkServer.RPCWriteChunk", gfs.WriteChunkArg{DataID: dataID, Offset: offset, Secondaries: lease.Secondaries}, &rep2)
	if rep2.ErrorCode == gfs.MutationResist {
		log.Info("[client]{WriteChunk} mutation resist, clear cache")
		c.leaseBuf.ClearCache()
	}
	if err != nil {
		return err
	} else {
		return nil
	}
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// len(data) should be within max append size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (gfs.Offset, error) {
	// log.Infof("[client]{AppendChunk} enter function, handle: %v; data: %s", handle, data)
	if len(data) > gfs.MaxAppendSize {
		return 0, gfs.Error{Code: gfs.UnknownError, Err: fmt.Sprintf("len(data) = %v > max append size %v", len(data), gfs.MaxAppendSize)}
	}

	lease, err := c.leaseBuf.GetLease(handle)
	if err != nil {
		return -1, gfs.Error{Code: gfs.UnknownError, Err: err.Error()}
	}
	dataID := chunkserver.NewDataID(handle)
	chain := append(lease.Secondaries, lease.Primary)
	// log.Info("[client]{AppendChunk} chain: ", chain)

	var rep gfs.ForwardDataReply
	err = util.Call(chain[0], "ChunkServer.RPCForwardData", gfs.ForwardDataArg{DataID: dataID, Data: data, AddrChain: chain[1:]}, &rep)
	if err != nil {
		return -1, gfs.Error{Code: gfs.UnknownError, Err: err.Error()}
	}

	var rep2 gfs.AppendChunkReply
	err = util.Call(lease.Primary, "ChunkServer.RPCAppendChunk", gfs.AppendChunkArg{DataID: dataID, Secondaries: lease.Secondaries}, &rep2)
	if rep2.ErrorCode == gfs.MutationResist {
		log.Info("[client]{AppendChunk} mutation resist, clear cache")
		c.leaseBuf.ClearCache()
	}
	if err != nil {
		return -1, gfs.Error{Code: gfs.UnknownError, Err: err.Error()}
	}
	if rep2.ErrorCode == gfs.AppendExceedChunkSize {
		return -1, gfs.Error{Code: gfs.AppendExceedChunkSize, Err: "append exceed chunk size"}
	}
	return rep2.Offset, nil
}

// // -------------------add feature-------------------
// func (c *Client) Delete(path gfs.Path) error {
// 	path = gfs.PathFormalizer(path, false)
// 	var rep gfs.DeleteFileReply
// 	err := util.Call(c.master, "Master.RPCDeleteFile", gfs.DeleteFileArg{Path: path}, &rep)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (c *Client) Rename(path gfs.Path, newPath gfs.Path) error {
// 	path = gfs.PathFormalizer(path, false)
// 	var rep gfs.RenameFileReply
// 	err := util.Call(c.master, "Master.RPCRenameFile", gfs.RenameFileArg{OldPath: path, NewPath: newPath}, &rep)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
