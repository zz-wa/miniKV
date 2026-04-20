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
		if in[0] == "set" {
			var ttl int64

			if len(in) != 3 && len(in) != 4 {
				conn.Write([]byte("wrong input" + "\n"))
				continue
			}
			if len(in) == 4 {
				ttl, _ = strconv.ParseInt(in[3], 10, 64)
			}
			if txn.IsActive() {
				txn.RecordFirst(in[1])
			}

			store.Set(in[1], in[2], ttl)

			conn.Write([]byte("ok\n"))
		}
		if in[0] == "get" {
			if len(in) < 2 {
				conn.Write([]byte("wrong input" + "\n"))
				continue
			}
			value, ok := store.Get(in[1])
			if !ok {
				conn.Write([]byte("key not found\n"))
			} else {
				conn.Write([]byte(value + "\n"))
			}

		}
		if in[0] == "del" {

			if len(in) < 2 {
				_, err := conn.Write([]byte("wrong input\n"))
				if err != nil {
					return
				}
				continue
			}
			if txn.IsActive() {
				txn.RecordFirst(in[1])
			}

			err := store.Del(in[1])
			if err != nil {
				_, err2 := conn.Write([]byte("err\n"))
				if err2 != nil {
					return
				}
				continue
			}
			_, err = conn.Write([]byte("ok\n"))
			if err != nil {
				return
			}
		}
		if in[0] == "begin" {
			err := txn.Begin()
			if err != nil {
				conn.Write([]byte(err.Error() + "\n"))
				continue
			}
			conn.Write([]byte("ok\n"))
			continue
		}
		if in[0] == "commit" {
			err := txn.Commit()
			if err != nil {
				conn.Write([]byte(err.Error() + "\n"))
				continue
			}
			conn.Write([]byte("ok\n"))
			continue
		}
		if in[0] == "rollback" {
			err := txn.Rollback()
			if err != nil {
				conn.Write([]byte(err.Error() + "\n"))
				continue
			}
			conn.Write([]byte("ok\n"))

			continue
		}
	}

}
