

# kv-store

一个用 Go 从零实现的 KV 存储引擎。

## 运行

```bash
go run main.go
```

用 `telnet` 或 `nc` 连接：

```bash
nc localhost 8080
```

支持的命令：
```
set key value
get key
del key
```

## 实现了什么

**WAL（Write-Ahead Log）**：每次写操作追加一条记录到文件末尾，而不是覆盖整个文件，写入压力更小。

**Compaction**：WAL 文件超过阈值时自动压缩，将当前完整状态重写为新文件，清除冗余历史记录。

**崩溃恢复**：启动时检测上次压缩是否完成，如果发现未完成的临时文件，自动恢复，保证数据不丢失。

**TCP 多客户端**：基于 `net` 标准库实现 TCP 服务器，每个连接用独立 goroutine 处理。

**并发安全**：使用 `sync.RWMutex`，读操作并发执行，写操作独占，保证多客户端同时读写的数据一致性。
