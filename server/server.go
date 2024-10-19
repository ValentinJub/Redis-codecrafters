package server

import (
	"fmt"
	"net"
	"os"
)

type Server interface {
	// Initialise the server creating a TCP listener
	Init()
	// Listen for TCP connections using our TCP listener
	Listen()
}

type MasterServer struct {
	address  string
	port     string
	listener net.Listener
}

func NewMasterServer(add, port string) *MasterServer {
	return &MasterServer{address: add, port: port}
}

func (s *MasterServer) Init() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.address, s.port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	s.listener = l
}

func (s *MasterServer) Listen() {
	// Event loop
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		connHandler := NewConnHandler(conn)
		go connHandler.HandleConnection()
	}
}
