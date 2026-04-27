
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

**Compaction**：WAL 超过阈值自动压缩，清除冗余历史记录，控制文件大小。

**崩溃恢复**：启动时检测上次压缩是否完成，自动恢复未完成的压缩，保证数据不丢失。

**索引**：内存只存 `key → 文件偏移量`，value 留在文件里，用 `ReadAt` 并发读取，内存占用小。

**TCP 多客户端**：基于 `net` 标准库，每个连接独立 goroutine 处理。

**并发安全**：`sync.RWMutex`，读并发执行，写独占。

**TTL**：支持给 key 设置过期时间，惰性删除 + 后台定时清理双策略。

**事务**：基于 undo-log 实现 `begin`/`commit`/`rollback`。事务中第一次修改某个 key 前备份原状态，`rollback` 时逐个还原，`commit` 直接清空备份；连接意外断开自动回滚未提交事务。

## 已知限制 / 待解决

- **key 和 value 不能含空格**：磁盘记录格式用空格分隔字段，含空格的 key/value 会导致解析截断。完整修法是改为 length-prefix 二进制格式（`| key_sz | val_sz | expireAt | key | val |`），目前暂不支持。

- **事务崩溃后原子性无法保证**：事务期间 Set/Del 立刻写磁盘，undo log 只在内存。崩溃后重启 replay 会使未 commit 的修改永久生效，违反 ACID 原子性。修法：事务期间修改缓存在内存，commit 时一次性写入；或写磁盘时加 BEGIN/COMMIT 标记，replay 时跳过没有 COMMIT 的事务。

- **goroutine 无上限**：每个 TCP 连接起一个 goroutine，连接洪峰时可能 OOM。修法：加最大连接数限制或 goroutine pool。

- **TTL 定期清理持全局锁**：`CleanupExpired` 扫描期间持 `mu.Lock()`，key 数量大时阻塞所有读写。修法：分批扫描，批间释放锁。

## 性能

SET QPS: ~73,000 | GET QPS: ~208,000

