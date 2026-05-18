# miniKV

一个用 Go 从零实现的 KV 存储引擎练习项目。

当前版本重点实现了 append-only WAL、内存索引、TTL、compaction、hint 文件、分片锁和简单 TCP 协议。代码仍然偏学习性质，README 里的“已知限制”部分会保留当前还没处理完的边界问题。

## 运行

启动服务：

```bash
go run main.go
```

用 `nc` 连接：

```bash
nc localhost 8080
```

默认监听：

```text
127.0.0.1:8080
```

数据文件默认写在当前工作目录：

```text
nosql.json
nosql.hint
nosql.tmp
```

## 支持的命令

```text
set key value          # 永久存储
set key value 60       # 60 秒后过期
get key                # 查询
del key                # 删除
begin                  # 开启事务
commit                 # 提交事务
rollback               # 回滚事务，撤销事务中的修改
```

示例：

```text
set name alice
ok
get name
alice
del name
ok
get name
key not found
```

## 当前实现

**Append-only WAL**

`Set` 和 `Del` 都会追加一条 record 到数据文件，不在原位置覆盖旧值。内存索引只保存最新 record 的位置，旧 record 后续由 compaction 清理。

**二进制 record 格式**

每条 record 使用 4 字节大端 body length 作为前缀，body 中包含：

```text
op | keyLen | valueLen | expireAt | key | value
```

因此磁盘 record 边界不依赖换行符，也不依赖空格切分。

**内存索引**

索引保存：

```text
key -> offset + length + expireAt
```

value 不常驻主索引。`Get` 通过 offset 和 length 从数据文件 `ReadAt` 读取 record。

**分片索引**

全局索引拆成 16 个 shard，按 key hash 路由。每个 shard 有自己的锁，减少单个全局 map 的锁竞争。

**TTL**

`set key value ttl` 会把过期时间写入 record 和内存索引。读取时会惰性检查过期；后台也会每 10 秒扫描 shard，删除已过期 key，并追加 delete record。

**Compaction**

当数据文件超过 100 KB 时，写入路径会异步尝试触发 compaction。compaction 会把当前仍有效的 key 重写到 `nosql.tmp`，成功后 rename 替换主数据文件，并重新打开读写文件句柄。

**Hint 文件**

compaction 后会生成 `nosql.hint`，保存 key 对应的 offset、length、expireAt，末尾写入 `DONE`。启动时如果认为 hint 完整，会优先从 hint 恢复索引；否则从主数据文件 replay。

**崩溃恢复**

启动时会检查是否存在残留的 `nosql.tmp`。如果存在，当前实现会直接删除这个临时文件，避免上次未完成的 compaction 临时文件影响启动。

**LRU 基础结构**

项目里实现了容量为 1000 的 LRU cache，`Set` 会写入 LRU，`Del` 和过期删除会移除 LRU 中的 key。当前 `Get` 路径还没有真正优先从 LRU 命中返回，这是后续要修的优化点。

**TCP 多连接**

服务端基于 `net` 标准库实现。每个连接由一个 goroutine 处理，并用容量 100 的 semaphore 限制同时处理的连接数量。

**事务**

事务基于内存 undo log 实现。事务中第一次修改某个 key 前，会通过 `GetMeta` 记录旧值；`rollback` 时按 undo log 还原，`commit` 时丢弃 undo log。连接断开时，如果事务还没提交，会自动 rollback。

## 并发模型

代码里主要有两层锁：

- `fileMu`：保护全局 `readFile`、`writeFile` 以及 compaction 时的文件句柄切换。
- shard 锁：保护每个 shard 内部的 `index` map。

普通读取会持有 `fileMu.RLock()`，复制索引 entry 后释放 shard 读锁，再通过 `ReadAt` 读取数据。写入、删除、过期清理和 compaction 会持有写锁，保证文件追加、索引更新和文件切换不会并发打架。

## 已知限制 / 待优化

- **LRU 还没有真正接入 Get 快路径**：当前 `Get` 仍然会先读磁盘，最后只调用一次 `lruCache.Get(key)` 调整顺序，返回值没有被使用。后续应把 `LRUCache.Get` 改成 `(string, bool)`，命中时直接返回，磁盘读取成功后再 `Put` 回 LRU。

- **TCP 协议不支持 key/value 中的空格和换行**：底层 record 已经是二进制格式，但网络协议仍用 `strings.Fields` 解析命令，并用换行作为请求边界。因此通过 TCP 写入时，key/value 不能包含空格或换行。

- **hint 完整性校验偏弱**：当前只要文件中出现一行 `DONE` 就认为 hint 完整。更稳妥的做法是要求 `DONE` 是最后一个有效行，或者给 hint 文件加 checksum。

- **hint 解析失败会直接 Fatal**：`loadIndexFromHint` 遇到 offset、length、expireAt 解析错误会退出进程。更合理的做法是返回 error，让 `Open` fallback 到 `rebuildIndexFromLog`。

- **rebuildIndexFromLog 遇到坏 record 会停止 replay**：如果中间某条 record 损坏，当前实现会 `break`，后面的有效 record 不会恢复。更稳妥的做法是能根据 length prefix 跳过坏 record 时继续扫描。

- **每次写都会启动 compaction goroutine**：`Set`/`Del` 末尾都会 `go Compaction(...)`，虽然内部有 CAS 防重入，但高频写入时会产生很多很快退出的 goroutine。后续可以改成常驻 worker + channel 触发。

- **compaction 错误处理还不完整**：部分文件操作错误没有完整清理资源，`Seek` 错误也还没处理。compaction 过程中还会提前更新内存 index，如果后续步骤失败，存在一致性风险。

- **Accept 错误被忽略**：`main.go` 里忽略了 `listen.Accept()` 的 error。listener 关闭或 fd 耗尽时，可能把 nil conn 传入 `HandleConn`。

- **TTL 参数解析错误被忽略**：`set a b abc` 这类输入会因为 `ParseInt` 错误被忽略而变成 ttl=0，也就是永不过期。后续应返回 `wrong input`。

- **事务没有隔离性**：事务期间其他连接仍然可以修改同一个 key。某个事务 rollback 时，可能覆盖其他连接已经提交的写入。

- **事务崩溃后不保证原子性**：undo log 只在内存中。事务中的 `Set`/`Del` 会立即写入 WAL，如果进程崩溃，重启 replay 时未 commit 的修改也可能生效。

- **没有 fsync 策略**：`Write` 成功不等于数据已经真正落盘。进程崩溃或机器断电时，最近写入可能丢失。

## 测试

运行全部测试：

```bash
go test ./...
```

运行 benchmark 程序：

```bash
go run bench/bench.go
```

当前 benchmark 结果会受到 LRU 是否接入 Get 快路径、compaction 触发频率、磁盘状态和本机环境影响，README 不再写固定 QPS，避免和代码状态脱节。
