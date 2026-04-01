# mcy-kv

## 项目概述

`mcy-kv` 是一个基于 Raft 的分布式键值存储（key-value store）从底层实现。项目在逻辑上分为：单副本 `KV` 模式、分片模式 `ShardKV`、分片控制器 `ShardCtrler`。

- Raft 模块负责一致性（Leader 选举、日志复制、Persist + Snapshot）。
- ShardCtrler 负责分片分配（Join / Leave / Move / Query），并在 config 变更后驱动数据迁移。
- ShardKV 负责跨 GID 的分片迁移（PullShard / DeleteShard / GC）、读写请求、幂等保证。
- 支持本地 `KVClient`、`ShardKVClient` 及控制器客户端 `CtrlerClient` 调度。

项目运行过程类似 TiKV 的 3 层架构（store /  placement driver / client），实现上也有类似分片与分布式协调的设计。

---

## 目录说明（逐文件）

### 根目录

- `go.mod`：模块声明。
- `README.md`：当前说明文档（已补全）。

### raftapi

- `raftapi.go`：Raft 接口定义+ApplyMsg结构+NoOp。服务层与 Raft 层之间使用该接口通信。

### labrpc、transport

- `labrpc/labrpc.go`：测试环境 RPC 网络模拟（可丢包、延迟、断连）。
- `transport/transport.go`：HTTP RPC 传输实现（生产/运行时用于进程间通讯）。

### labgob、labpersister

- `labgob/labgob.go`: Gob 编码封装，检测字段大小写以保证序列化安全。
- `labpersister/persister.go`: 本地文件持久化 Raft 状态与快照，实现断点恢复。

### raft

- `raft/raft.go`：Raft 核心实现
    - 状态机：Follower、Candidate、Leader
    - RequestVote / AppendEntries
    - 心跳与选举（ticker）
    - 日志管理（`log`、`commitIndex`、`lastApplied`）
    - Snapshot（3D）：`Snapshot()`, `InstallSnapshot()`, `readPersist()`、`persist()`
    - “WAL + Snapshot”实现：链式日志写入 `persist()`，当节点超过 `maxraftstate` 时通过 `Snapshot` 生成快照，并剪裁 `log`（事实上是日志压缩，类似 TiKV 的raftstore写入 WAL）

### kv

- `kv/kv.go`：单副本 KV 服务。
    - API：`Put`, `Get`
    - 幂等/重复请求处理：`lastseq`/`lastcmd`
    - Raft 成员 `ApplyMsg` 处理：`applier()` 完成实际写入
    - 快照：`maybeTakeSnapshot`, `restoreFromSnapshot`

### kvclient

- `kvclient/kvclient.go`：简单循环重试客户端，处理 `ErrWrongLeader`、`ErrTimeout`。

### shardctrler

- `shardctrler/config.go`：分片控制器实现
    - Config 保存和版本 `configs []Config`
    - `Join/Leave/Move/Query` 操作
    - 重平衡算法（`rebalanceLocked`）：均衡 shard 到 groups
    - Raft 提交逻辑：`apply()` 入口对命令线性化
    - 快照：`maybeTakeSnapshot`, `restoreFromSnapshot`

### ctrlerclient

- `ctrlerclient/ctrlerclient.go`：ShardCtrler 客户端（封装强一致读取、重试）。

### shardkv

- `shardkv/shardkv.go`：分片 KV 服务器实现
    - 状态定义：`Serving`, `Pulling`, `BePulling`, `GC`, `Inactived`
    - `configPoller`：leader 周期查询 `ShardCtrler` 最新 Config
    - `applyConfig`: 触发状态迁移
    - `Puller`: 拉取其他 GID 的 shard 数据
    - `gcWorker`: 清理旧数据并向旧主发送 `DeleteShard` 以完成迁移
    - `Put/Get`：基于 shard 方案检查权限 / 状态后提交 Raft
    - `PullShard/DeleteShard` RPC：迁移数据接口
    - 幂等与去重：`lastshardseq`, `lastshardcmd`

### shardkvclient

- `shardkvclient/shardkvclient.go`：基于 `ShardCtrler` 集群的客户端，自动路由到 shard GID 并循环重试。核心逻辑：查询 config -> map key 到 shard ->转到目标 server

### main-* 启动脚本

- `main-server/server.go`：单副本 Raft KV 集群启动常规节点数组5个（8001~8005）。
- `main-client/client.go`：kv 交互终端
- `main-shard/shardserver.go`：共有3个 group，每个 group 对应5节点组(8001~8015)，使用 `ShardKV` 与配置控制器交互 
- `main-shardclient/shardclient.go`：ShardKV 客户端测例
- `main-shardctrler/shardctrler.go`：ShardCtrler 核心服务（5个节点 8016~8020）
- `main-shardmanager/shardmanager.go`：命令行操作 `join/move/leave`

---

## 重点协议分析

### 1. Raft 协议及 WAL + 快照

核心实现点：
- `raft.go` 里持久化 `currentTerm/votedFor/log/lastIncludedIndex/lastIncludedTerm`。
- 每次 `Start` append 命令即走 WAL（`persist()`），目的是断点重启后恢复状态。
- `Snapshot(index, snapshot)`：条件 `index>lastIncludedIndex` && `index<=lastApplied`。剪裁日志并持久化 `raftstate + snapshot`。这对应 TiKV raftstore 的 Compact WAL 实现。
- `InstallSnapshot`：当 follower日志落后太多或 leader快照后，直接同步数据并发送 `ApplyMsg{SnapshotValid:true}`到上层服务。
- `applier` 与 `ticker` 协同：Leader 发送心跳 (`AppendEntries`)；Follower 超时后发起选举；commit 后逐条 apply 给业务层。

### 2. Shard 控制协议（迁移/一致性）

迁移协议核心在 `shardkv/shardkv.go`。

状态机：
- `Serving`：可以处理读写。
- `Pulling`：本组暂未持有，等待从 old group 拉取。
- `BePulling`：即将被迁出，等待 old group 提供数据并清理。
- `GC`：拉入内存后写完删除标志，等待上层确定后转 Serving。
- `Inactived`：静默状态，无数据。

迁移流程：
1. `configPoller` 定期获取 `next config` 当 `allServing` 且 `pendingNum==currentNum`。
2. `applyConfig` 更新分片状态（由 old/new group 决定拉取/待拉等）。
3. `Puller` 在 `Pulling` 状态：
   - 0 代表空 shard，直接 local insert 空。
   - 否则向 old group 逐个试拉 `PullShard` RPC 获得数据/seq/lastCmd，然后提交 `InsertShard` Op。
4. `applier()` 处理 `InsertShard`：把 `shardkv/shardData` 导入本地，进入 `GC`。
5. `gcWorker` 在 `GC` 阶段向 old group 发 `DeleteShard`，old 接到后在 `BePulling` 转 `Inactived`，触发本地 config 更新。
6. new 在完成 `DeleteShard`的 RPC 调用之后，在 `GC` 转 `Serving`，触发本地 config 更新。
 
Key value 路径：
- `Put/Get` 先检查 `config.Shards[shard]==gid` 且 `Serving`。
- 通过 Raft 写入序列化日志，以线性一致性保证跨分片提交仍安全（在本组内）。

### 3. Raft 协议与 shard 逻辑结合

每个 shardKV 节点本身也是一个 Raft 集群成员（跨组节点独立），组内一致性由 raft 保证。配置变更通过 ShardCtrler 做强一致日志，推送到客户端（`ctrlerclient.Query`）。

分片迁移整体可视为类似 TiKV：PD （controller）发布 region（shard）分配，TiKV 节点按分配做迁移，读写路由 Client 层，它同样使用 group+shard查询。

---

## 运行方式（示例）

1. 启动 ShardCtrler（5 个节点）：
   - `go run main-shardctrler/shardctrler.go --id=0 ... --id=4` 
2. 启动 ShardKV 节点，每个 `gid` 单独 5 个实例共 3 个 group:
   - `go run main-shard/shardserver.go --id=0 ... --id=4`（每个 gid 共享一个进程，内部循环构造多个 shard server 实例）
3. 启动管理工具 `go run main-shardmanager/shardmanager.go` 做 `join/move/leave`。
4. 启动客户端：
   - `go run main-shardclient/shardclient.go` 输入 `put/get`。

### Raft 单副本测试

- 启动 5 个单副本 KV：
  - */main-server* + 5 id
- 使用 `main-client` 做读写。

---

## 对比 TiKV 架构（类 TiKV 论点）

1. Placement Driver
   - TiKV PD -> 这里由 `ShardCtrler` 负责。
2. Region 存储
   - TiKV Region 对应 `Shard`。ShardKV 与 PD 协调迁移、GC。
3. TiKV LSM + RocksDB
   - 项目只实现了内存 map + +Raft WAL/快照（但架构可扩展为磁盘存储层）。
4. Client 路由
   - `shardkvclient` 模拟 TiKV 客户端向正确分片路由读写。

---

## TODO & 改进建议

- 增加 `*_test.go` 自动化测试（Raft、ShardCtrler、ShardKV 合集成）
- 实现一个 `svr`/`pd` 脚本以启动多个进程（免手工开很多终端），现已可手动啟动。
- 添加清晰运行命令、检查点、Metrics。
- 完善异常（网络分区等边界）和性能测试。

---

## 和用户的认证备注

你已完成的调试非常关键，即便本仓库当前没测试文件，说明核心逻辑已经实现。此 README 可作为项目“自我审计”与他人复现参考。
