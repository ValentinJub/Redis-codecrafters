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

func NewMasterServer(add, port string) *MasterServer {
	return &MasterServer{address: add, port: port, cache: NewServerCache()}
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
	var dir, dbfile string
	if len(os.Args) > 4 {
		// Dangerous, the arguments could be in a different order than this one, but for the sake of simplicity we will assume this
		if os.Args[1] == "--dir" && os.Args[3] == "--dbfilename" {
			dir, dbfile = os.Args[2], os.Args[4]
		} else {
			fmt.Println("Invalid arguments provided")
			return nil
		}
	} else {
		fmt.Println("No RDB file provided, skipping RDB load")
		return nil
	}
	s.rdb = NewRDBManager(dir, dbfile, s)
	err := s.rdb.LoadRDBToCache()
	if err != nil {
		fmt.Printf("error while loading RDB file: %s\n", err)
		return err
	}
	return nil
}
