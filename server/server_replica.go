package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"

	utils "github.com/codecrafters-io/redis-starter-go/utils"
)

type ReplicaServer interface {
	RedisServer
	HandleMasterConnection()
	SyncWithMaster()
	SendToMaster(data []byte) error
	ReadFromMaster() ([]byte, error)
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
	fmt.Printf("Replica RedisServer created with address: %s:%s and RDB info dir: %s file: %s\n", server.address, server.port, dir, dbfile)
	return server
}

func (r *ReplicaServerImpl) SendToMaster(data []byte) error {
	_, err := r.masterConn.Write(data)
	return err
}

func (r *ReplicaServerImpl) ReadFromMaster() ([]byte, error) {
	buf := make([]byte, 1024)
	n, err := r.masterConn.Read(buf)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Received data from master: %s\n", buf[:n])
	return buf[:n], nil
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
		go s.HandleClientConnections(conn)
	}
}

// Handle incoming TCP Requests
func (s *ReplicaServerImpl) HandleClientConnections(conn net.Conn) {
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
		reqHandler := NewRequestHandler(request, s)
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

	expect := func(got, want []byte) bool {
		if !reflect.DeepEqual(got, want) {
			fmt.Printf("Expected '%s' but got '%s'\n", want, got)
			return false
		}
		return true
	}

	// Send the PING command
	r.SendToMaster(newBulkArray("PING"))

	// Read the response
	resp, err := r.ReadFromMaster()
	if err != nil {
		fmt.Println("Error reading from master during handshake PING: ", err.Error())
		os.Exit(1)
	}
	if !expect(resp, newSimpleString("PONG")) {
		fmt.Println("Error syncing with master")
		os.Exit(1)
	}

	// Send the 1st REPLCONF commands
	r.SendToMaster(newBulkArray("REPLCONF", "listening-port", r.port))

	// Read the response
	resp, err = r.ReadFromMaster()
	if err != nil {
		fmt.Println("Error reading from master during handshake 1st REPLCONF: ", err.Error())
		os.Exit(1)
	}
	if !expect(resp, newSimpleString("OK")) {
		fmt.Println("Error syncing with master")
		os.Exit(1)
	}

	// Send the 2nd REPLCONF commands
	r.SendToMaster(newBulkArray("REPLCONF", "capa", "psync2"))

	// Read the response
	resp, err = r.ReadFromMaster()
	if err != nil {
		fmt.Println("Error reading from master during handshake 2nd REPLCONF: ", err.Error())
		os.Exit(1)
	}
	if !expect(resp, newSimpleString("OK")) {
		fmt.Println("Error syncing with master")
		os.Exit(1)
	}

	// Send the PSYNC command
	r.SendToMaster(newBulkArray("PSYNC", "?", "-1"))
	r.handlePostHandshakeData()
	return nil
}

func (r *ReplicaServerImpl) handlePostHandshakeData() error {
	// Read the response
	resp, err := r.ReadFromMaster()
	if err != nil {
		fmt.Println("Error reading from master: ", err.Error())
		os.Exit(1)
	}

	// Decipher the response. It can be a mix of FULLRESYNC and the RDB and one or more commands
	// We will need to decipher it like a stream of bytes whether we have handled the RDB or not
	data := resp
	cursor := 0
	// Our goal is to find the end bound of the FULLRESYNC message which ends with \n
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			cursor = i
			fmt.Printf("FULLRESYNC: '%s'\n", data[:cursor])
			break
		}
	}

	// Set the replicationID and the offset
	parts := strings.Split(string(data[:cursor]), " ")
	r.replicationID = parts[1]

	fmt.Printf("Replication ID set: %s\n", r.replicationID)

	// The RDB wasn't sent with the FULLRESYNC message, attempt to read it
	if cursor+1 == len(data) {
		buf := make([]byte, 1024)
		n, err := r.masterConn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from master: ", err.Error())
			os.Exit(1)
		}
		fmt.Printf("Received response from master: %s\n", buf[:n])
		data = buf[:n]
		cursor = 0
		// The RDB file isn't received yet
		if len(data) == 0 {
			fmt.Println("No data received")
			return nil
		}
	} else {
		cursor++
	}

	// We need to handle the RDB
	lenToignore := 0
	// The RDB length is prefixed with a '$' character, grab the length and ignore the RDB
	if data[cursor] == '$' {
		str := ""
		for i := cursor + 1; i < len(data); i++ {
			if data[i] != '\r' {
				str += string(data[i])
			} else {
				cursor = i
				break
			}
		}
		lenToignore, err = strconv.Atoi(str)
		if err != nil {
			fmt.Println("Error converting RDB length: ", err)
			return nil
		}
	} else {
		fmt.Printf("Unexpected character: '%c'\n", data[cursor])
		return nil
	}

	// Ignore the RDB
	cursor += lenToignore + 2
	if cursor >= len(data) {
		fmt.Printf("RDB ignored and end of data\n")
		return nil
	}

	// Handle the rest of the data
	fmt.Printf("Data remaining: %s\n", data[cursor:])
	reqH := NewReqHandlerMasterReplica(data[cursor:], r)
	go reqH.HandleRequest()
	return nil
}
