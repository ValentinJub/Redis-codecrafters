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

func (r *ReplicaServer) GetMaster() *MasterServer {
	return nil
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
	r.doHandshake(conn)
}

func (r *ReplicaServer) doHandshake(conn net.Conn) {
	// Send the PING command
	conn.Write(newBulkArray("PING"))

	// Read the response
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		os.Exit(1)
	}
	fmt.Printf("Received response from master: %s\n", buf[:n])
	if string(buf[:n]) != "+PONG"+CRLF {
		fmt.Println("Error syncing with master")
		os.Exit(1)
	}

	// Send the REPLCONF commands
	conn.Write(newBulkArray("REPLCONF", "listening-port", r.port))
	n, err = conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		os.Exit(1)
	}
	fmt.Printf("Received response from master: %s\n", buf[:n])
	if string(buf[:n]) != "+OK"+CRLF {
		fmt.Println("Error syncing with master")
		os.Exit(1)
	}
	conn.Write(newBulkArray("REPLCONF", "capa", "psync2"))
	n, err = conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		os.Exit(1)
	}
	fmt.Printf("Received response from master: %s\n", buf[:n])
	if string(buf[:n]) != "+OK"+CRLF {
		fmt.Println("Error syncing with master")
		os.Exit(1)
	}

	// Send the PSYNC command
	conn.Write(newBulkArray("PSYNC", "?", "-1"))
	n, err = conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		os.Exit(1)
	}
	fmt.Printf("Received response from master: %s\n", buf[:n])
	// if string(buf[:n]) != "+FULLRESYNC"+CRLF {
	// 	fmt.Println("Error syncing with master")
	// 	os.Exit(1)
	// }

}
