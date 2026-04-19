package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type entry struct {
	Offset   int64
	Length   int64
	ExpireAt int64
}

var noSQL = make(map[string]entry)

var WFile *os.File
var RFile *os.File
var mu sync.RWMutex

func main() {
	_, err := os.Stat("nosql.tmp")
	if err == nil {
		err = os.Rename("nosql.tmp", "nosql.json")
		if err != nil {
			return
		}
	}

	readFile, err := os.ReadFile("nosql.json")

	lines := strings.Split(string(readFile), "\n")
	offset := 0
	for _, line := range lines {

		if line == "" {
			offset += 1
			continue
		}
		l := strings.Split(line, " ")

		if l[0] == "set" {
			expireAt, err := strconv.ParseInt(l[3], 10, 64)
			if err != nil {
				offset += len(line) + 1
				continue
			}
			if expireAt != 0 && time.Now().Unix() > expireAt {
				offset += len(line) + 1
				continue
			}

			noSQL[l[1]] = entry{
				Offset:   int64(offset),
				Length:   int64(len(line) + 1),
				ExpireAt: expireAt,
			}
		}

		if l[0] == "del" {
			delete(noSQL, l[1])
		}
		offset += len(line) + 1

	}

	if err != nil && !os.IsNotExist(err) {
		return
	}

	listen, err := net.Listen("tcp", ":8080")
	WFile, _ = os.OpenFile("nosql.json", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	RFile, _ = os.OpenFile("nosql.json", os.O_RDONLY, 0644)
	go CleanupExpired()

	for {
		conn, _ := listen.Accept()
		go handleConn(conn)
	}

}

func CleanupExpired() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		mu.Lock()
		for key, en := range noSQL {
			if en.ExpireAt != 0 && en.ExpireAt <= time.Now().Unix() {
				delete(noSQL, key)
				WFile.Write([]byte("del " + key + " \n"))
			}
		}
		mu.Unlock()
	}
}
func handleConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		in := strings.Split(input, " ")
		if in[0] == "set" {
			var expiredAt int64

			if len(in) != 3 && len(in) != 4 {
				conn.Write([]byte("wrong input" + "\n"))
				continue
			}
			if len(in) == 4 {
				ttl, err := strconv.ParseInt(in[3], 10, 64)
				if err != nil {
					conn.Write([]byte("wrong input" + "\n"))
					continue
				}
				expiredAt = time.Now().Unix() + ttl
			}
			lines := fmt.Sprintf("set %s %s %d\n", in[1], in[2], expiredAt)
			mu.Lock()
			offset, _ := WFile.Seek(0, io.SeekEnd)
			en := entry{
				Offset:   offset,
				Length:   int64(len(lines)),
				ExpireAt: expiredAt,
			}
			noSQL[in[1]] = en

			WFile.Write([]byte(lines))

			if Compaction("nosql.json", noSQL) {
				WFile.Close()
				WFile, _ = os.OpenFile("nosql.json", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				RFile.Close()
				RFile, _ = os.OpenFile("nosql.json", os.O_RDONLY, 0644)

			}
			mu.Unlock()
			conn.Write([]byte("ok\n"))
		}
		if in[0] == "get" {
			if len(in) < 2 {
				conn.Write([]byte("wrong input" + "\n"))
				continue
			}
			mu.RLock()
			en, ok := noSQL[in[1]]
			if !ok {
				conn.Write([]byte("key not found" + "\n"))
				mu.RUnlock()
				continue
			}
			if en.ExpireAt != 0 && time.Now().Unix() > en.ExpireAt {
				mu.RUnlock()
				mu.Lock()
				en2, ok2 := noSQL[in[1]]
				if !ok2 {
					conn.Write([]byte("key not found" + "\n"))
					mu.Unlock()
					continue
				}
				if ok2 && en2.ExpireAt != 0 && time.Now().Unix() > en2.ExpireAt {
					delete(noSQL, in[1])
					WFile.Write([]byte("del " + in[1] + " \n"))
				}
				conn.Write([]byte("key not found" + "\n"))
				mu.Unlock()
				continue
			}

			buf := make([]byte, en.Length)
			_, err := RFile.ReadAt(buf, en.Offset)
			if err != nil && err != io.EOF {
				conn.Write([]byte("key not found" + "\n"))
				mu.RUnlock()
				continue
			}
			l := strings.SplitN(string(buf), " ", 4)
			if len(l) < 3 {
				conn.Write([]byte("key not found" + "\n"))
				mu.RUnlock()
				continue
			}

			conn.Write([]byte(strings.TrimSpace(l[2]) + "\n"))

			mu.RUnlock()
		}
		if in[0] == "del" {

			if len(in) < 2 {
				conn.Write([]byte("wrong input"))
				continue
			}
			mu.Lock()
			delete(noSQL, in[1])
			WFile.Write([]byte("del " + in[1] + " \n"))
			WFile.Write([]byte(input + "\n"))
			if Compaction("nosql.json", noSQL) {
				WFile.Close()
				WFile, _ = os.OpenFile("nosql.json", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				RFile.Close()
				RFile, _ = os.OpenFile("nosql.json", os.O_RDONLY, 0644)

			}
			mu.Unlock()
		}
	}

}

func Compaction(filename string, nosql map[string]entry) bool {
	fi, _ := os.Stat(filename)
	size := fi.Size()
	if size > 100000 {
		WFile, _ = os.OpenFile("nosql.tmp", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		for key, _ := range nosql {
			en := noSQL[key]
			buf := make([]byte, en.Length)
			_, err := RFile.ReadAt(buf, en.Offset)
			if err != nil && err != io.EOF {
				return false
			}
			l := strings.SplitN(string(buf), " ", 3)
			if len(l) < 3 {
				continue
			}
			newLine := fmt.Sprintf("set %s %s\n", key, strings.TrimSpace(l[2]))

			newOffset, _ := WFile.Seek(0, io.SeekEnd)

			noSQL[key] = entry{
				Offset:   newOffset,
				Length:   int64(len(newLine)),
				ExpireAt: en.ExpireAt,
			}
			WFile.Write([]byte(newLine))
		}
		os.Remove(filename)
		os.Rename("nosql.tmp", "nosql.json")
		return true
	}
	return false
}
