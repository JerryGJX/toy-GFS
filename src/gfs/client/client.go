package client

import (
	"fmt"
	"gfs"
	"gfs/util"
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
	var rep gfs.CreateFileReply
	err := util.Call(c.master, "Master.RPCCreateFile", gfs.CreateFileArg{Path: path}, &rep)
	if err != nil {
		return err
	}
	return nil
}

// Mkdir creates a new directory on GFS.
func (c *Client) Mkdir(path gfs.Path) error {
	var rep gfs.MkdirReply
	err := util.Call(c.master, "Master.RPCMkdir", gfs.MkdirArg{Path: path}, &rep)
	if err != nil {
		return err
	}
	return nil
}

// List lists everything in specific directory on GFS.
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error) {
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
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {
	var info gfs.GetFileInfoReply
	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &info)
	if err != nil {
		return -1, err
	}
	index := int64(offset / gfs.MaxChunkSize)
	if index > info.Chunks{
		return -1, fmt.Errorf("[Read] error: offset is out of range")
	}
	pos := 0
	
	return 0, nil
}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	return nil
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	return offset, nil
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	return 0, nil
}

// ReadChunk reads data from the chunk at specific offset.
// len(data)+offset  should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	return 0, nil
}

// WriteChunk writes data to the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	return nil
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// len(data) should be within max append size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	return offset, nil
}

// -------------------add feature-------------------
func (c *Client) Delete(path gfs.Path) error {
	return nil
}

func (c *Client) Rename(path gfs.Path, newPath gfs.Path) error {
	return nil
}
