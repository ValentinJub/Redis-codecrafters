package server

import (
	"fmt"
	"os"
)

type ReplicaServer struct {
	Server
	masterAddress string
}

func NewReplicaServer(args map[string]string) RedisServer {
	port, ok := args["--port"]
	if !ok {
		port = SERVER_PORT
	}
	dir, ok := args["--dir"]
	if !ok {
		dir = ""
	}
	dbfile, ok := args["--dbfilename"]
	if !ok {
		dbfile = ""
	}
	replicaof, ok := args["--replicaof"]
	if !ok {
		fmt.Println("Missing argument for --replicaof")
		os.Exit(1)
	}
	server := &ReplicaServer{Server: Server{role: "slave", address: SERVER_ADDR, port: port, cache: NewCache(), replicationID: createReplicationID()}, masterAddress: replicaof}
	server.rdb = NewRDBManager(dir, dbfile, server)
	fmt.Printf("RedisServer created with address: %s:%s and RDB info dir: %s file: %s\n", server.address, server.port, dir, dbfile)
	return server
}
