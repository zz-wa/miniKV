package main

import (
	"kv/server"
	"kv/store"
	"log"
	"net"
)

func main() {
	if err := store.Open("nosql.json"); err != nil {
		log.Fatal(err)
	}
	listen, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	var sem = make(chan struct{}, 100)

	for {
		conn, _ := listen.Accept()
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			server.HandleConn(conn)

		}()
	}

}
