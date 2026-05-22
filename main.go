package main

import (
	"flag"
	"kv/config"
	"kv/server"
	"kv/store"
	"log"
	"net"
)

func main() {
	configPath := flag.String("config", "./config/config.yaml", "path to config file")
	flag.Parse()
	cfg := config.ReadConf(*configPath)

	if err := store.Open(cfg.Store); err != nil {
		log.Fatal(err)
	}
	listen, err := net.Listen("tcp", cfg.Server.Addr)
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
