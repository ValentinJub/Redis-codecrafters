package main

import (
	"github.com/codecrafters-io/redis-starter-go/server"
)

const (
	SERVER_ADDR = "127.0.0.1"
	SERVER_PORT = "6379"
)

func main() {
	server := server.NewMasterServer(SERVER_ADDR, SERVER_PORT)
	server.LoadRDBToCache()
	server.Init()
	server.Listen()
}
