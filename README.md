# toy-GFS

> a simple implementation of The Google File System

## workspace setup

```
.
├── go.work
├── go.work.sum
└── toy-GFS
    ├── README.md
    ├── doc
    │   └── ppca2016
    ├── go.mod
    ├── graybox_test.go
    ├── llgfs.go
    ├── servers.txt
    ├── src
    │   ├── gfs
    │   │   ├── chunkserver
    │   │   │   ├── chunkserver.go
    │   │   │   ├── download_buffer.go
    │   │   │   └── go.mod
    │   │   ├── client
    │   │   │   ├── client.go
    │   │   │   └── go.mod
    │   │   ├── common.go
    │   │   ├── go.mod
    │   │   ├── master
    │   │   │   ├── chunk_manager.go
    │   │   │   ├── chunkserver_manager.go
    │   │   │   ├── go.mod
    │   │   │   ├── master.go
    │   │   │   └── namesapce_manager.go
    │   │   ├── rpc_structs.go
    │   │   └── util
    │   │       ├── array_set.go
    │   │       ├── go.mod
    │   │       └── util.go
    │   ├── gfs_stress
    │   │   ├── atomic_append_success.go
    │   │   ├── consistency_write_success.go
    │   │   ├── fault_tolerance.go
    │   │   └── stress.go
    │   └── github.com
    │       └── Sirupsen
    │           └── logrus
    │               ├── CHANGELOG.md
    │               ├── LICENSE
    │               ├── README.md
    │               ├── alt_exit.go
    │               ├── alt_exit_test.go
    │               ├── doc.go
    │               ├── entry.go
    │               ├── entry_test.go
    │               ├── examples
    │               │   ├── basic
    │               │   │   └── basic.go
    │               │   └── hook
    │               │       └── hook.go
    │               ├── exported.go
    │               ├── formatter.go
    │               ├── formatter_bench_test.go
    │               ├── go.mod
    │               ├── go.sum
    │               ├── hook_test.go
    │               ├── hooks
    │               │   ├── syslog
    │               │   │   ├── README.md
    │               │   │   ├── syslog.go
    │               │   │   └── syslog_test.go
    │               │   └── test
    │               │       ├── test.go
    │               │       └── test_test.go
    │               ├── hooks.go
    │               ├── json_formatter.go
    │               ├── json_formatter_test.go
    │               ├── logger.go
    │               ├── logrus.go
    │               ├── logrus_test.go
    │               ├── terminal_bsd.go
    │               ├── terminal_linux.go
    │               ├── terminal_notwindows.go
    │               ├── terminal_solaris.go
    │               ├── terminal_windows.go
    │               ├── text_formatter.go
    │               ├── text_formatter_test.go
    │               └── writer.go
    └── stress
        ├── go.mod
        ├── stress_center.go
        └── stress_node.go

```



In order not to switch the `#GOPATH` , we use the `go module` and `go workspace` as alteration. 

Each directory is a go module, and all the used go module should be add to the `go.work` file.

``` go
//go.work
go 1.18
use (
	./toy-GFS/src/gfs
	./toy-GFS/src/gfs/chunkserver
	./toy-GFS/src/gfs/client
	./toy-GFS/src/gfs/master
	./toy-GFS/src/gfs/util
	./toy-GFS/src/github.com/Sirupsen/logrus
    ./toy-GFS
	./toy-GFS/stress
)
```

