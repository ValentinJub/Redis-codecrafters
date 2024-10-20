package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/server"
)

func main() {

	server := server.NewMasterServer("127.0.0.1", "6379")

	if len(os.Args) > 4 {
		if os.Args[1] == "--dir" && os.Args[3] == "--dbfilename" {
			dir, dbfile := os.Args[2], os.Args[4]
			server.SetRDBConfig(dir, dbfile)
			fmt.Println("RDB configuration set")
		}
	}

	server.Init()
	server.Listen()
}
