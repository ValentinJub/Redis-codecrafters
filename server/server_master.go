package server

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

type MasterServer interface {
	RedisServer
	AddReplica(addr string, r net.Conn)
	GetReplicas() map[string]net.Conn
	Propagate(req *Request)
	SendRDBFile(conn net.Conn) error
}

type MasterServerImpl struct {
	RedisServerImpl
	replicas map[string]net.Conn
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
	server := &MasterServerImpl{RedisServerImpl: RedisServerImpl{role: "master", address: SERVER_ADDR, port: port, cache: NewCache(), replicationID: utils.CreateReplicationID()}, replicas: make(map[string]net.Conn)}
	server.rdb = NewRDBManager(dir, dbfile, server)
	fmt.Printf("Master RedisServerImpl created with address: %s:%s and RDB info dir: %s file: %s\n", server.address, server.port, dir, dbfile)
	return server
}

func (s *MasterServerImpl) GetReplicas() map[string]net.Conn {
	return s.replicas
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
		go s.HandleConnection(conn)
	}
}

// Handle incoming TCP Requests
func (s *MasterServerImpl) HandleConnection(conn net.Conn) {
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
	_, err = conn.Write(append([]byte(fmt.Sprintf("$%d\r\n", len(content))), content...))
	if err != nil {
		return err
	}
	return nil
}
