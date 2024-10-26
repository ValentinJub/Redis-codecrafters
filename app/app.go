package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/server"
)

func main() {
	args, err := parseOsArgs(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	serverManager := server.NewServerManager(args)
	server := serverManager.SpwanServer()
	server.LoadRDBToCache()
	server.Init()
	server.Listen()
}

func parseOsArgs(args []string) (map[string]string, error) {
	argsMap := make(map[string]string)
	for x, arg := range args {
		switch arg {
		case "--dir":
			if x+1 < len(args) {
				argsMap[arg] = args[x+1]
				fmt.Printf("dir: %s\n", args[x+1])
			} else {
				return nil, fmt.Errorf("missing argument for --dir")
			}
		case "--dbfilename":
			if x+1 < len(args) {
				argsMap[arg] = args[x+1]
				fmt.Printf("dbfilename: %s\n", args[x+1])
			} else {
				return nil, fmt.Errorf("missing argument for --dbfilename")
			}
		case "--port":
			if x+1 < len(args) {
				argsMap[arg] = args[x+1]
				fmt.Printf("port: %s\n", args[x+1])
			} else {
				return nil, fmt.Errorf("missing argument for --port")
			}
		case "--replicaof":
			if x+1 < len(args) {
				parts := strings.Split(args[x+1], " ")
				if parts[0] == "localhost" {
					parts[0] = "127.0.0.1"
				}
				argsMap[arg] = strings.Join(parts, ":")
				fmt.Printf("replicaof: %s\n", argsMap[arg])
			} else {
				return nil, fmt.Errorf("missing argument for --replicaof")
			}
		default:
		}
	}
	return argsMap, nil
}
