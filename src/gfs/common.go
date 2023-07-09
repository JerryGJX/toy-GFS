package gfs

import (
	// "gfs"
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
	Secondaries []ServerAddress
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
	LeaseExpire        = 2 * time.Second //1 * time.Minute
	HeartbeatInterval  = 100 * time.Millisecond
	BackgroundInterval = 200 * time.Millisecond //
	ServerTimeout      = 1 * time.Second        //

	MaxChunkSize  = 512 << 10 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize = MaxChunkSize / 4

	DefaultNumReplicas = 3
	MinimumNumReplicas = 2

	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 10 * time.Second

	DeletedFilePrefix = "_delete_"
)
