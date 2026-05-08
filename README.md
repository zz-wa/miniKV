
# miniKV

一个用 Go 从零实现的 KV 存储引擎。

## 运行

```bash
go run main.go
```

用 `nc` 连接：

```bash
nc localhost 8080
```

## 支持的命令

```
set key value          # 永久存储
set key value 60       # 60秒后过期
get key                # 查询
del key                # 删除
begin                  # 开启事务
commit                 # 提交事务
rollback               # 回滚事务，撤销事务中的所有修改
```

## 实现了什么

**WAL（Write-Ahead Log）**：每次写操作只追加一条记录，不覆盖整个文件，写入压力小。

**Length-prefix 磁盘格式**：每条记录前 4 字节用大端写 body 长度，再写 body。重放时按长度切片，record 边界不再依赖换行符，单条记录的 value 可以安全包含 `\n`。

**Compaction**：WAL 超过阈值（100 KB）异步触发压缩，重写一份只包含最新值的 tmp 文件，结尾写 `DONE` 标记后原子 rename 替换。

**崩溃恢复**：启动时检测 `nosql.tmp` 是否包含 `DONE` 标记，完整就替换主文件，否则丢弃。保证压缩中途崩溃也不会污染数据。

**Hint 文件**：每次 compaction 同时写一份 `nosql.hint`（key + offset + length + expireAt），尾部带 `DONE`。下次启动若 hint 完整，直接用 hint 重建内存索引，跳过对主数据文件的全量扫描。

**分片索引（Sharding）**：内存索引切成 16 个 shard，按 FNV hash 路由。每个 shard 各自持锁，并发写不同 shard 互不阻塞。

**LRU 热点缓存**：写入和读取后顺手填一份到容量 1000 的 LRU，热点 key 命中时直接走内存。

**索引**：内存只存 `key → 文件偏移量 + 长度 + 过期时间`，value 留在文件里，用 `ReadAt` 并发读取，内存占用小。

**TCP 多客户端 + 连接数上限**：基于 `net` 标准库，每个连接独立 goroutine 处理；用容量 100 的 semaphore 限制最大并发连接，防止连接洪峰打爆 goroutine。

**并发安全**：`sync.RWMutex` 分两层 —— 全局 `fileMu` 保护文件句柄切换（compaction 会替换 `writeFile`/`readFile`），shard 级锁保护各自的索引 map。读并发执行，写独占。

**TTL**：支持给 key 设置过期时间，惰性删除（`Get` 时检查并删除）+ 后台定时清理（每 10s 扫描一次）双策略。

**事务**：基于 undo-log 实现 `begin`/`commit`/`rollback`。事务中第一次修改某个 key 前用 `GetMeta` 备份原状态，`rollback` 时逐个还原，`commit` 直接清空备份；连接意外断开自动回滚未提交事务。

## 已知限制 / 待解决

- **key 和 value 不能含空格**：record 边界已经靠 length-prefix 稳定，但 record 内部 body 仍用 `strings.SplitN(body, " ", 4)` 解析，含空格的 key/value 会切错字段。完整修法是把 body 也改成结构化二进制格式（`| key_sz | val_sz | expireAt | key | val |`）。

- **事务崩溃后原子性无法保证**：事务期间 Set/Del 立刻写磁盘，undo log 只在内存。崩溃后重启 replay 会使未 commit 的修改永久生效，违反 ACID 原子性。修法：事务期间修改缓存在内存，commit 时一次性写入；或写磁盘时加 BEGIN/COMMIT 标记，replay 时跳过没有 COMMIT 的事务。

- **TTL 定期清理仍持 shard 写锁**：`CleanupExpired` 现在按 shard 持锁，阻塞范围已经从全局缩到 1/16，但单个 shard 在扫描期间仍阻塞该 shard 的所有读写。修法：分批扫描，批间释放锁。

- **每次写都启动一个 compaction goroutine**：`Set`/`Del` 末尾的 `go Compaction(...)` 内部用 `CompareAndSwap` 防止并发执行，但写入压力大时会持续创建 goroutine 然后立刻退出。修法：用单独的 compaction 触发器（条件变量 / channel）按需唤醒。

## 性能

SET QPS: ~58,000 | GET QPS: ~215,000

(`bench/bench.go`，10 个写连接 × 100 ops，50 个读连接 × 1000 ops。三次取稳定值。SET 比早期下降，主要原因是改成 length-prefix 后多了一次 header 写、且每次写都尝试触发 compaction；GET 因为命中 LRU 路径基本持平。)