package main

import (
	"github.com/codecrafters-io/redis-starter-go/server"
)

func main() {
	server := server.NewMasterServer("127.0.0.1", "6379")
	server.Init()
	server.Listen()
}
