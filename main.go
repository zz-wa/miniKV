package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

type entry struct {
	Offset int64
	Length int64
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

			noSQL[l[1]] = entry{
				Offset: int64(offset),
				Length: int64(len(line) + 1),
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
	for {
		conn, _ := listen.Accept()
		go handleConn(conn)
	}

}

func handleConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		in := strings.Split(input, " ")
		if in[0] == "set" {
			length := len(input)

			if len(in) < 3 {
				conn.Write([]byte("wrong input" + "\n"))
				continue
			}
			mu.Lock()
			offset, _ := WFile.Seek(0, io.SeekEnd)
			en := entry{
				Offset: offset,
				Length: int64(length + 1),
			}
			noSQL[in[1]] = en

			WFile.Write([]byte(input + "\n"))

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

			buf := make([]byte, en.Length)
			_, err := RFile.ReadAt(buf, en.Offset)
			if err != nil && err != io.EOF {
				conn.Write([]byte("key not found" + "\n"))
				mu.RUnlock()
				continue
			}
			l := strings.Split(string(buf), " ")
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
			l := strings.Split(string(buf), " ")
			if len(l) < 3 {
				continue
			}
			newLine := fmt.Sprintf("set %s %s\n", key, strings.TrimSpace(l[2]))

			newOffset, _ := WFile.Seek(0, io.SeekEnd)

			noSQL[key] = entry{
				Offset: newOffset,
				Length: int64(len(newLine)),
			}
			WFile.Write([]byte(newLine))
		}
		os.Remove(filename)
		os.Rename("nosql.tmp", "nosql.json")
		return true
	}
	return false
}
