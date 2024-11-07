package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/server"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

func main() {
	args, err := utils.ParseOsArgs(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	serverManager := server.NewServerManager(args)
	server := serverManager.SpawnServer()
	server.LoadRDBToCache()
	server.Init()
	server.Listen()
}
