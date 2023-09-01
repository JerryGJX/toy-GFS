# toy-GFS

> a simple implementation of The Google File System

## 1. workspace setup

```shell
.
├── README.md
├── doc
│   ├── GFS_notes.md
│   └── ppca2016.md
├── servers.txt
└── src
    ├── gfs
    │   ├── chunkserver
    │   │   ├── chunkserver.go
    │   │   └── download_buffer.go
    │   ├── client
    │   │   ├── client.go
    │   │   └── leaseBuffer.go
    │   ├── cmd
    │   │   └── main.go
    │   ├── common.go
    │   ├── go.mod
    │   ├── go.sum
    │   ├── graybox_test.go
    │   ├── master
    │   │   ├── chunk_manager.go
    │   │   ├── chunkserver_manager.go
    │   │   ├── master.go
    │   │   └── namesapce_manager.go
    │   ├── rpc_structs.go
    │   └── util
    │       ├── array_set.go
    │       └── util.go
    └── gfs_stress
        ├── atomic_append_success.go
        ├── cmd
        │   ├── center
        │   │   └── stress_center.go
        │   └── node
        │       └── stress_node.go
        ├── consistency_write_success.go
        ├── fault_tolerance.go
        ├── go.mod
        └── stress.go

```

In order not to switch the `#GOPATH` , we use the `go module` and `go workspace` as alteration.

```go
//go.work
go 1.18
use (
	./toy-GFS/src/gfs
	./toy-GFS/src/gfs_stress
)
```

## 2. testing

### 2.1 gray box test

- how to run the test

  ```bash
  cd (prefix path)/toy-GFS/src/gfs
  go test
  ```

### 2.2 pressure test

- how to run the test (not sure)

## 3. design

### 3.1 client API 实现

#### Create: create a new file on the specific path

```go
func (c *Client) Create(path gfs.Path) error
```

realization:

- ask master to create a new file by calling `Master.RPCCreateFile`

#### Mkdir: create a new directory on a specific path

```go
func (c *Client) Mkdir(path gfs.Path) error
```

realization:

- ask master to create a new directory by calling `Master.RPCMkdir`

#### List: lists everything in specific directory

```go
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error)
```

realization:

- acquire the info by calling `Master.RPCList` and get the reply

#### Read: reads the file at specific offset

> - reads up to len(data) bytes form the File
> - return the number of bytes, and an error if any

```go
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (int, error)
```

realization:

- get the file info (if a directory, length of file and chunk number of file) by calling `MAster.RPCGetFileInfo`

- check if the offset is too large

- recursively read chunk

  - use file path and chunk index to get chunk handle by calling `Master.RPCGetChunkHandle`

  - call `ReadChunk` to read context in chunk level

    ```go
    func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error)
    ```

    - call `Master.RPCGetReplicas` to get all the address of chunkserver that hold the replicas of the chunk
    - randomly choose a chunkserver and call `ChunkServer.RPCReadChunk`



#### Write: writes data to the file at specific offset

```go
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error
```

realization:

- get the file info (if a directory, length of file and chunk number of file) by calling `MAster.RPCGetFileInfo`

- check if the offset is too large

- recursively write chunk

  - use file path and chunk index to get chunk handle by calling `Master.RPCGetChunkHandle`

  - call `WriteChunk` to write context in chunk level

    ```go
    func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error 
    ```

    - call `leaseBuf.GetLease` to get the info of primary and secondary replicas
    - call `ChunkServer.RPCForwardData` to send the data in a string to all the related chunkserver
    - call `ChunkServer.RPCWriteChunk` of the primary chunkserver to deliver the instruction
    - if the chunkserver is under `mutation lock` mode, then we clear the lease buffer

#### Append: appends data to the file

```go
func (c *Client) Append(path gfs.Path, data []byte) (gfs.Offset, error) 
```

- get the file info (if a directory, length of file and chunk number of file) by calling `MAster.RPCGetFileInfo`

- recursively append chunk

  - use file path and chunk index to get chunk handle by calling `Master.RPCGetChunkHandle`

  - call `AppendChunk` to write context in chunk level

    ```go
    func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (gfs.Offset, error)
    ```

    - call `leaseBuf.GetLease` to get the info of primary and secondary replicas
    - call `NewDataID` to generate a data ID according to the time
    - call `ChunkServer.RPCForwardData` to send the data in a string to all the related chunkserver
    - call `ChunkServer.RPCAppendChunk` of the primary chunkserver to deliver the instruction
    - if the chunkserver is under `mutation lock` mode, then we clear the lease buffer



### 3.2 feature function

#### reReplicate: to maintain the number of replica of a chunk

```go
func (m *Master) reReplication(handle gfs.ChunkHandle) error
```

- get the `from` and `to` server address by calling `chunkserverManager.ChooseRereplication`
- create a chunk at the `to` server by calling `Chunkserver.RPCCreateChunk`
- let the `from` server send the info to the `to` server by calling `ChunkServer.RPCSendCopy`
- call `chunkManager.RegisterReplica` to add the `to` server address to the meta data
- call `chunkServerManager.AddChunk` to add the chunk handle to the handle list of `to` server

#### snapshot: to backup a part of metadate

```go
func (m *Master) Snapshot(rootPath gfs.Path, storageFileName string) error 
```

- call `namespaceManager.ListRelatedFile` to get the path list of all the related files
- call `chunkManager.GetRelatedChunk` to get the chunkhandle list of all the related chunks
- call `chunkManager.ClearRelatedLease` to clear all the leases of related chunks
- call `chunkManager.GetRelatedChunkServer` to get all the addresses of the chunkservers that hold replicas of related chunks
- call `master.SetChunkServerStartSnapshot` to inform all the related chunkservers
- respectively serialize the metadata in namespaceManager and chunkManager and store to the given file
- call `master.SetChunkServerEndSnapshot` to inform all the related chunkservers

### 3.2. extra feature

> design and arrangement that are differ from the description in the paper

1. requirement of the path in the GFS:
   for all: start with `/`
   for directory: end up with `/`
   for file: end up with filename
