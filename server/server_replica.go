package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	utils "github.com/codecrafters-io/redis-starter-go/utils"
)

type ReplicaServer interface {
	RedisServer
	SyncWithMaster()
	HandleMasterConnection()
}

type ReplicaServerImpl struct {
	RedisServerImpl
	masterAddress string
	masterConn    net.Conn
}

func NewReplicaServer(args map[string]string) *ReplicaServerImpl {
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
	server := &ReplicaServerImpl{RedisServerImpl: RedisServerImpl{role: "slave", address: SERVER_ADDR, port: port, cache: NewCache(), replicationID: utils.CreateReplicationID()}, masterAddress: replicaof}
	server.rdb = NewRDBManager(dir, dbfile, server)
	fmt.Printf("Slave RedisServerImpl created with address: %s:%s and RDB info dir: %s file: %s\n", server.address, server.port, dir, dbfile)
	return server
}

// Override the Init method to allow the replica to sync with the master
func (r *ReplicaServerImpl) Init() {
	r.SyncWithMaster()
	r.RedisServerImpl.Init()
}

// Event loop, handles requests inside it
func (s *ReplicaServerImpl) Listen() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go s.HandleConnection(conn)
	}
}

// Handle incoming TCP Requests
func (s *ReplicaServerImpl) HandleConnection(conn net.Conn) {
	buff := make([]byte, 1024)
	for {
		// Read from the connection
		bytesRead, err := conn.Read(buff)
		if err != nil {
			if errors.Is(err, io.EOF) {
				conn.Close()
				break
			}
			fmt.Println(err)
			return
		}
		// The data read from the TCP stream
		request := buff[:bytesRead]
		// Handles the decoded request and produce an answer
		reqHandler := NewReqHandlerReplica(request, s)
		response := reqHandler.HandleRequest()

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println(err)
			break
		}
	}
}

func (r *ReplicaServerImpl) dialMaster() {
	conn, err := net.Dial("tcp", r.masterAddress)
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		os.Exit(1)
	}
	r.masterConn = conn
}

func (r *ReplicaServerImpl) SyncWithMaster() {
	fmt.Printf("Syncing with master: %s\n", r.masterAddress)
	r.dialMaster()
	err := r.doHandshake()
	if err != nil {
		fmt.Println("Error syncing with master: ", err)
		os.Exit(1)
	}
	fmt.Printf("Synced with master: %s\n", r.masterAddress)
	go r.HandleMasterConnection()
}

func (r *ReplicaServerImpl) HandleMasterConnection() {
	buff := make([]byte, 1024)
	for {
		// Read from the connection
		bytesRead, err := r.masterConn.Read(buff)
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.masterConn.Close()
				break
			}
			fmt.Println(err)
			return
		}
		// The data read from the TCP stream
		request := buff[:bytesRead]
		// Handles the decoded request and produce an answer
		reqHandler := NewReqHandlerMasterReplica(request, r)
		reqHandler.HandleRequest()
	}
}

func (r *ReplicaServerImpl) doHandshake() error {
	// Send the PING command
	r.masterConn.Write(newBulkArray("PING"))

	// Read the response
	buf := make([]byte, 1024)
	n, err := r.masterConn.Read(buf)
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
	r.masterConn.Write(newBulkArray("REPLCONF", "listening-port", r.port))
	n, err = r.masterConn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		os.Exit(1)
	}
	fmt.Printf("Received response from master: %s\n", buf[:n])
	if string(buf[:n]) != "+OK"+CRLF {
		fmt.Println("Error syncing with master")
		os.Exit(1)
	}
	r.masterConn.Write(newBulkArray("REPLCONF", "capa", "psync2"))
	n, err = r.masterConn.Read(buf)
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
	r.masterConn.Write(newBulkArray("PSYNC", "?", "-1"))
	n, err = r.masterConn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		os.Exit(1)
	}
	fmt.Printf("Received response from master: %s\n", buf[:n])
	// if string(buf[:n]) != "+FULLRESYNC"+CRLF {
	// 	fmt.Println("Error syncing with master")
	// 	os.Exit(1)
	// }
	// n, err = r.masterConn.Read(buf)
	// if err != nil {
	// 	fmt.Println("Error reading from master: ", err.Error())
	// 	os.Exit(1)
	// }
	fmt.Printf("Received response from master: %s\n", buf[:n])
	return nil
}
