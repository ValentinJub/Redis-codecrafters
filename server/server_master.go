package server

import (
	"fmt"
	"net"
)

type MasterServer struct {
	Server
}

func NewMasterServer(args map[string]string) *MasterServer {
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
	server := &MasterServer{Server: Server{role: "master", address: SERVER_ADDR, port: port, cache: NewCache(), replicationID: createReplicationID(), replicas: make(map[net.Conn]bool)}}
	server.rdb = NewRDBManager(dir, dbfile, server)
	fmt.Printf("Master Server created with address: %s:%s and RDB info dir: %s file: %s\n", server.address, server.port, dir, dbfile)
	return server
}

// func (m *MasterServer) GetReplicas() map[string]bool {
// 	return m.replicas
// }
