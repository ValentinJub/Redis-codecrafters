package server

import (
	"fmt"
	"net"
	"os"
)

type Server interface {
	// Initialise the server creating a TCP listener
	Init()
	// Listen for TCP connections using our TCP listener.
	// Encapsulates the request handling process
	Listen()
}

type MasterServer struct {
	address  string
	port     string
	listener net.Listener
	cache    Cache
	rdb      RDBManager
}

const (
	SERVER_ADDR = "127.0.0.1"
	SERVER_PORT = "6379"
)

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
	server := &MasterServer{address: SERVER_ADDR, port: port, cache: NewServerCache()}
	server.rdb = NewRDBManager(dir, dbfile, server)
	fmt.Printf("Server created with address: %s:%s and RDB info dir: %s file: %s\n", server.address, server.port, dir, dbfile)
	return server
}

// Initialise the server, creating a listener
func (s *MasterServer) Init() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.address, s.port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	s.listener = l
}

// Event loop, handles requests inside it
func (s *MasterServer) Listen() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		connHandler := NewConnHandler(conn, s)
		go connHandler.HandleConnection()
	}
}

// Loads the RDB file into the cache
func (s *MasterServer) LoadRDBToCache() error {
	d, f := s.rdb.Info()
	if d == "" || f == "" {
		fmt.Printf("Invalid RDB file path: '%s/%s', skipping loading\n", d, f)
		return nil
	}
	err := s.rdb.LoadRDBToCache()
	if err != nil {
		fmt.Printf("error while loading RDB file: %s\n", err)
		return err
	}
	return nil
}
