# toy-GFS

> a simple implementation of The Google File System

## 1. workspace setup

```shell
.
├── go.work
└── toy-GFS
    ├── README.md
    ├── doc
    │   ├── GFS_notes.md
    │   ├── assets
    │   │   ├── image-20230628142224412.png
    │   │   └── image-20230628142256374.png
    │   └── ppca2016
    ├── servers.txt
    └── src
        ├── gfs
        │   ├── chunkserver
        │   │   ├── chunkserver.go
        │   │   └── download_buffer.go
        │   ├── client
        │   │   └── client.go
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

Each directory is a go module, and all the used go module should be add to the `go.work` file.

```go
//go.work
go 1.18
use (
	./toy-GFS/src/gfs
	./toy-GFS/src/gfs_stress
)
```

## 2. testing

### 2.1. gray box test

- how to run the test

  ```bash
  cd (prefix path)/toy-GFS/src/gfs
  go test
  ```

### 2.2. pressure test

## 3. design

### 3.1. major structure

### 3.2. extra feature

> design and arrangement that are differ from the description in the paper

1. requirement of the path in the GFS:
   for all: start with `/`
   for directory: end up with `/`
   for file: end up with filename
