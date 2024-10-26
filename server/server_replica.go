package server

import (
	"fmt"
	"net"
	"os"
)

type ReplicaServer struct {
	Server
	masterAddress string
}

func NewReplicaServer(args map[string]string) *ReplicaServer {
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
	fmt.Printf("Slave Server created with address: %s:%s and RDB info dir: %s file: %s\n", server.address, server.port, dir, dbfile)
	return server
}

func (r *ReplicaServer) Init() {
	r.SyncWithMaster()
	r.Server.Init()
}

func (r *ReplicaServer) SyncWithMaster() {
	fmt.Printf("Syncing with master: %s\n", r.masterAddress)

	// Connect to the master
	conn, err := net.Dial("tcp", r.masterAddress)
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	// Send the PING command
	conn.Write(newBulkArray("PING"))
}
