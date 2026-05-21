# miniKV

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

当数据文件超过 100 KB 时，写入路径会异步尝试触发 compaction。compaction 流程拆成三步：`shouldCompact` 判断是否需要重写，`buildCompactedFile` 把当前仍有效的 key 写到 `nosql.tmp` 并在临时 slice 里构建新索引，`writeHintFromIndex` 最后输出 hint 文件。tmp 写完后 rename 替换主数据文件，重新打开读写文件句柄，再一次性把新索引 swap 到各 shard。

**Hint 文件**

compaction 后会生成 `nosql.hint`，保存 key 对应的 offset、length、expireAt，末尾写入 `DONE`。启动时如果认为 hint 完整，会优先从 hint 恢复索引；否则从主数据文件 replay。

**崩溃恢复**

启动时会检查是否存在残留的 `nosql.tmp`。如果存在，当前实现会直接删除这个临时文件，避免上次未完成的 compaction 临时文件影响启动。

**LRU 缓存**

项目里实现了容量为 1000 的 LRU cache 并接入了读路径：`Get` 命中索引后会先查 LRU，命中直接返回；miss 才走 `ReadAt` 读磁盘，并把结果回写到 LRU。`Set` 写入新值时也会更新 LRU，`Del` 和过期删除会移除对应 key。

**TCP 多连接**

服务端基于 `net` 标准库实现。每个连接由一个 goroutine 处理，并用容量 100 的 semaphore 限制同时处理的连接数量。

**事务**

事务基于内存 undo log 实现。事务中第一次修改某个 key 前，会通过 `GetMeta` 记录旧值；`rollback` 时按 undo log 还原，`commit` 时丢弃 undo log。连接断开时，如果事务还没提交，会自动 rollback。

## 并发模型

代码里主要有两层锁：

- `fileMu`：保护全局 `readFile`、`writeFile` 以及 compaction 时的文件句柄切换。
- shard 锁：保护每个 shard 内部的 `index` map。

普通读取会持有 `fileMu.RLock()`，复制索引 entry 后释放 shard 读锁，再通过 `ReadAt` 读取数据。写入、删除、过期清理和 compaction 会持有写锁，保证文件追加、索引更新和文件切换不会并发打架。
