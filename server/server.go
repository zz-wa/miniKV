package server

import (
	"bufio"
	"kv/store"
	"net"
	"strconv"
	"strings"
)

func HandleConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		input, _ := reader.ReadString('\n')
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
	}

}
