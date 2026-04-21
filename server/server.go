package server

import (
	"bufio"
	"kv/store"
	"net"
	"strconv"
	"strings"
)

func HandleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	txn := &Txn{}
	defer func() {
		if txn.IsActive() {
			txn.Rollback()
		}
	}()
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		input = strings.TrimSpace(input)
		in := strings.Split(input, " ")
		switch in[0] {
		case "set":
			handleSet(conn, in, txn)
		case "get":
			handleGet(conn, in)
		case "del":
			handleDel(conn, in, txn)
		case "begin":
			handleBegin(conn, txn)
		case "commit":
			handleCommit(conn, txn)
		case "rollback":
			handleRollback(conn, txn)
		default:
			conn.Write([]byte("unknown command\n"))
		}
	}
}
func handleSet(conn net.Conn, in []string, txn *Txn) {
	var ttl int64

	if len(in) != 3 && len(in) != 4 {
		conn.Write([]byte("wrong input\n"))
		return
	}
	if len(in) == 4 {
		ttl, _ = strconv.ParseInt(in[3], 10, 64)
	}
	if txn.IsActive() {
		txn.RecordFirst(in[1])
	}

	err := store.Set(in[1], in[2], ttl)
	reply(conn, err)
}
func handleGet(conn net.Conn, in []string) {
	if len(in) < 2 {
		conn.Write([]byte("wrong input\n"))
		return
	}
	value, ok := store.Get(in[1])
	if !ok {
		conn.Write([]byte("key not found\n"))
	} else {
		conn.Write([]byte(value + "\n"))
	}

}
func handleDel(conn net.Conn, in []string, txn *Txn) {
	if len(in) < 2 {
		conn.Write([]byte("wrong input\n"))
		return
	}
	if txn.IsActive() {
		txn.RecordFirst(in[1])
	}

	err := store.Del(in[1])
	reply(conn, err)

}

func handleBegin(conn net.Conn, txn *Txn) {
	err := txn.Begin()
	reply(conn, err)
}
func handleCommit(conn net.Conn, txn *Txn) {
	err := txn.Commit()
	reply(conn, err)

}
func handleRollback(conn net.Conn, txn *Txn) {
	err := txn.Rollback()
	reply(conn, err)

}
func reply(conn net.Conn, err error) {
	if err != nil {
		conn.Write([]byte(err.Error() + "\n"))
		return
	}
	conn.Write([]byte("ok\n"))
}
