package server

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

type MasterServer interface {
	RedisServer
	AddReplica(addr string, r net.Conn)
	GetReplicas() map[string]net.Conn
	Propagate(req *Request)
	SendRDBFile(conn net.Conn) error
	Wait(req *Request) []byte
	CacheRequest(req *Request)
	GetReplicationBacklog() map[int]Request
	AddAckReceived()
	ResetAckReceived()
}

type MasterServerImpl struct {
	RedisServerImpl
	replicas map[string]net.Conn
	// The replication backlog keeps track of the requests that need to be propagated to the replicas
	// The key is the offset of the request in the replication stream
	replicationBacklog map[int]Request
	acksReceived       int
}

func NewMasterServer(args map[string]string) *MasterServerImpl {
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
	server := &MasterServerImpl{RedisServerImpl: RedisServerImpl{
		role: "master", address: SERVER_ADDR, port: port, cache: NewCache(), replicationID: utils.CreateReplicationID(), QueuedRequests: make(map[string][]Request)},
		replicas:           make(map[string]net.Conn),
		replicationBacklog: make(map[int]Request),
	}
	server.rdb = NewRDBManager(dir, dbfile, server)
	fmt.Printf("Master RedisServerImpl created with address: %s:%s and RDB info dir: %s file: %s\n", server.address, server.port, dir, dbfile)
	return server
}

func (s *MasterServerImpl) AddAckReceived() {
	s.acksReceived++
}

func (s *MasterServerImpl) ResetAckReceived() {
	s.acksReceived = 0
}

func (s *MasterServerImpl) GetReplicas() map[string]net.Conn {
	return s.replicas
}

func (s *MasterServerImpl) GetReplicationBacklog() map[int]Request {
	return s.replicationBacklog
}

func (s *MasterServerImpl) CacheRequest(req *Request) {
	s.replicationBacklog[s.replicationOffset] = *req
}

func (s *MasterServerImpl) AddReplica(addr string, r net.Conn) {
	s.replicas[addr] = r
}

func (s *MasterServerImpl) Propagate(req *Request) {
	for addr, replica := range s.replicas {
		_, err := replica.Write(newBulkArray(append([]string{req.command}, req.args...)...))
		if err != nil {
			fmt.Printf("Error writing to %s replica: %s\n", addr, err)
		}
	}
}

// Event loop, handles requests inside it
func (s *MasterServerImpl) Listen() {
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
func (s *MasterServerImpl) HandleClientConnections(conn net.Conn) {
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
		reqHandler := NewReqHandlerMaster(request, s, conn)
		response := reqHandler.HandleRequest()

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println(err)
			break
		}
	}
}

// Send an RDB file to a Replica
func (s *MasterServerImpl) SendRDBFile(conn net.Conn) error {
	fmt.Printf("Sending RDB file to replica\n")
	dir, dbfile := s.rdb.RDBInfo()
	buffer, err := utils.ReadFile(dir + "/" + dbfile)
	if err != nil {
		fmt.Printf("Error reading RDB file: %s\n", err)
		buffer.Reset()
		// Craft an empty RDB file
		data, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
		if err != nil {
			fmt.Printf("Error decoding hex string: %s\n", err)
			return err
		}
		buffer.Write(data)
	}
	content := buffer.Bytes()
	// Sleep 1 ms to prevent the replica receiving the RDB file before the FULLRESYNC command
	time.Sleep(1 * time.Millisecond)
	_, err = conn.Write(append([]byte(fmt.Sprintf("$%d\r\n", len(content))), content...))
	if err != nil {
		return err
	}
	return nil
}

/*
WAIT numreplicas timeout

This command blocks the current client until all the previous write commands are successfully transferred and acknowledged
by at least the number of replicas specified in the numreplicas argument.
If the value you specify for the timeout argument (in milliseconds) is reached, the command returns even if the specified number of replicas were not yet reached.

The command will always return the number of replicas that acknowledged the write commands sent by the current client before the WAIT command
both in the case where the specified number of replicas are reached, or when the timeout is reached.
*/
func (s *MasterServerImpl) Wait(req *Request) []byte {
	if len(req.args) < 2 {
		return newSimpleString("WAIT command requires at least 2 arguments (count of replicas and timeout)")
	}
	numOfReplicas, err := strconv.Atoi(req.args[0])
	if err != nil {
		return newSimpleString("Error parsing replicas count")
	}
	timeout, err := strconv.Atoi(req.args[1])
	if err != nil {
		return newSimpleString("Error parsing timeout")
	}

	if numOfReplicas == 0 {
		// We can return immediately since the number of replicas is 0
		return newInteger(0)
	} else if s.replicationOffset == 0 {
		// Return the number of known replicas
		return newInteger(len(s.GetReplicas()))
	}

	fmt.Printf("Waiting for %d replicas with %dms timeout\n", numOfReplicas, timeout)

	start := time.Now().UnixMilli()
	end := start + int64(timeout)

	/*
		Sends the REPLCONF GETACK command to all the replicas,
		some replicas may answer or not, the number of ACKs received is counted
		in the RequestHandlerMaster.replicationConfig method
	*/
	for _, replica := range s.replicas {
		go replica.Write(newBulkArray("REPLCONF", "GETACK", "*"))
	}

	// The replication offset is incremented by 37 bytes for the REPLCONF GETACK command sent
	s.replicationOffset += 37

	// Count the number of ACKs received from the replicas
	for s.acksReceived < numOfReplicas {
		if time.Now().UnixMilli() > end {
			fmt.Printf("Timeout reached\n")
			break
		}
	}

	ackCopy := s.acksReceived
	s.ResetAckReceived()
	return newInteger(ackCopy)
}
