package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

var noSQL = make(map[string]string)
var file *os.File
var mu sync.RWMutex

func handleConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		in := strings.Split(input, " ")
		if in[0] == "set" {

			if len(in) < 3 {
				fmt.Println("wrong input ")
				continue
			}
			mu.Lock()
			noSQL[in[1]] = in[2]

			file.Write([]byte(input + "\n"))
			if Compaction("nosql.json", noSQL) {
				file.Close()
				file, _ = os.OpenFile("nosql.json", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			}
			mu.Unlock()
		}
		if in[0] == "get" {
			if len(in) < 2 {
				conn.Write([]byte("wrong input"))
				continue
			}
			mu.RLock()

			conn.Write([]byte(noSQL[in[1]]))
			mu.RUnlock()
		}
		if in[0] == "del" {

			if len(in) < 2 {
				conn.Write([]byte("wrong input"))
				continue
			}
			mu.Lock()
			delete(noSQL, in[1])
			file.Write([]byte(input + "\n"))
			if Compaction("nosql.json", noSQL) {
				file.Close()
				file, _ = os.OpenFile("nosql.json", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

			}
			mu.Unlock()

		}

	}

}

// 实现kv存储
func main() {
	_, err := os.Stat("nosql.tmp")
	if err == nil {
		os.Rename("nosql.tmp", "nosql.json")
	}

	readFile, err := os.ReadFile("nosql.json")

	lines := strings.Split(string(readFile), "\n")
	for _, line := range lines {

		if line == "" {
			continue
		}

		l := strings.Split(line, " ")

		if l[0] == "set" {
			noSQL[l[1]] = l[2]
		}

		if l[0] == "del" {
			delete(noSQL, l[1])
		}

	}

	if err != nil && !os.IsNotExist(err) {
		return
	}

	listen, err := net.Listen("tcp", ":8080")
	file, _ = os.OpenFile("nosql.json", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	for {
		conn, _ := listen.Accept()
		go handleConn(conn)
	}

}

func Compaction(filename string, nosql map[string]string) bool {
	fi, _ := os.Stat(filename)
	size := fi.Size()
	if size > 10 {
		file, _ := os.OpenFile("nosql.tmp", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		for key, value := range nosql {

			fmt.Fprintf(file, "set %s %s\n", key, value)

		}
		os.Remove(filename)
		os.Rename("nosql.tmp", "nosql.json")
		return true
	}
	return false
}
