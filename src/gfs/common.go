package gfs

import (
	// "gfs"
	// "errors"
	"fmt"
	"strings"
	"time"
)

type Path string
type ServerAddress string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type ChunkVersion int64
type Checksum int64
type MutationType int

type DataBufferID struct {
	Handle    ChunkHandle
	TimeStamp int
}

type PathInfo struct {
	Name string

	// if it is a directory
	IsDir bool

	// if it is a file
	Length int64
	Chunks int64
}

type SplitPath struct {
	IsDir bool
	Parts []string
}

type Lease struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress //since each time refresh a lease, the secondaries will be updated, so we can store it in lease
}

type PersistentChunkInfo struct {
	Handle   ChunkHandle
	Length   Offset
	Version  ChunkVersion
	Checksum Checksum
}

func (sp *SplitPath) SplitPath2Path() Path {
	ret := ""
	if len(sp.Parts) == 0 {
		return Path("/")
	} else {
		for _, part := range sp.Parts {
			ret += "/" + part
		}
		if sp.IsDir {
			ret += "/"
		}
	}
	return Path(ret)
}

func (sp *SplitPath) ParentSp() (*SplitPath, error) {
	if len(sp.Parts) == 0 {
		return nil, fmt.Errorf("[ParentSp] error: root has no parent")
	} else {
		return &SplitPath{IsDir: true, Parts: sp.Parts[:len(sp.Parts)-1]}, nil
	}
}

func (p Path) Path2SplitPath() *SplitPath {
	ret := SplitPath{IsDir: false, Parts: []string{}}
	ps := strings.Split(string(p), "/")
	if ps[0] == "" {
		ps = ps[1:]
	}
	if ps[len(ps)-1] == "" {
		ps = ps[:len(ps)-1]
		ret.IsDir = true
	}
	ret.Parts = ps
	return &ret
}

func PathFormalizer(path Path, isDir bool) Path {
	sp := path.Path2SplitPath()
	sp.IsDir = isDir
	return sp.SplitPath2Path()
}

const (
	MutationWrite = iota
	MutationAppend
	MutationPad
)

type ErrorCode int

const (
	Success = iota
	UnknownError
	AppendExceedChunkSize
	WriteExceedChunkSize
	ReadEOF
	NotAvailableForCopy
	MutationResist
)

// extended error type with error code
type Error struct {
	Code ErrorCode
	Err  string
}

func (e Error) Error() string {
	return e.Err
}

// system config
const (
	// chunk
	LeaseExpire        = 3 * time.Second
	DefaultNumReplicas = 3
	MinimumNumReplicas = 2
	MaxChunkSize       = 512 << 10 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize      = MaxChunkSize / 4
	DeletedFilePrefix  = "_delete_"

	//master
	ServerCheckInterval = 500 * time.Millisecond
	MasterStoreInterval = 30 * time.Hour
	ServerTimeout       = 1 * time.Second

	//chunk server
	HeartbeatInterval    = 100 * time.Millisecond
	MutationWaitTimeout  = 4 * time.Second
	ServerStoreInterval  = 40 * time.Hour // 30 * time.Minute
	GarbageCollectionInt = 30 * time.Hour // 1 * time.Day
	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 10 * time.Second

	CheckPointInterval = 30 * time.Minute

	//for client
	ClientTimeout   = 2*LeaseExpire + 3*ServerTimeout
	LeaseBufferTick = 500 * time.Millisecond

	//for snapshot
	StartSnapshot = true
	EndSnapshot   = false
)
