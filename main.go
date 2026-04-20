package main

import (
	"kv/server"
	"kv/store"
	"log"
	"net"
)

func main() {
	if err := store.Init("nosql.json"); err != nil {
		log.Fatal(err)
	}
	listen, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, _ := listen.Accept()
		go server.HandleConn(conn)
	}

}
