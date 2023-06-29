# notes for GFS

 ![image-20230628142256374](assets/image-20230628142256374.png)

## 1. 使用场景

- system 由不可靠的商品化部件构成
- 该 system 主要存储适当数量的大文件，系统主要要针对 Multi-GB files 做优化
- workload 主要为 large streaming reads 和 small random read
- workload 中的 write 主要为 appending data，且写完后很少改动
- 要用尽可能少的 synchronization 实现 atomicity 以此实现多 client 的 concurrent 操作
- high sustained bandwidth > low latency



## 2. Interfaces

- **usual operation:** create, delete, open, close, read, write
- **special operation**
  - **snapshot:** create a copy of a file or a directory tree at low cost
  - **record append:** 在保证原子性的前提下支持多个 client 并行地向同一个文件 append 内容

### 2.1. Snapshot



### 2.2. Record append





## 3. Architecture

- single master
- multiple chunk-servers
- multiple clients



### 3.1. chunk related

> - files are divided into fixed-size chunks

#### 3.1.1. chunk usage

- **storage:** chunk-servers store chunks on local disks
- **read & write:** chunk-servers conduct the operation specified by a ***chunk handle and byte range***



#### 3.1.2. chunk handle

> the identifier of a chunk

- **property:** 64bit, immutable（不可改变的）, global unique

- **assignment:** assigned by the master at the time of chunk creation



#### 3.1.3. chunk size

> choose 64 MB

- 





## 1. GFS master

### 1.1 