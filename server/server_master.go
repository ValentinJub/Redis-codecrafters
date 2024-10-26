package server

import (
	"fmt"
	"net"
	"os"
)

type MasterServer struct {
	role     string
	address  string
	port     string
	listener net.Listener
	cache    Cache
	rdb      RDBManager
}

func NewMasterServer(args map[string]string) RedisServer {
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
	server := &MasterServer{role: "master", address: SERVER_ADDR, port: port, cache: NewServerCache()}
	server.rdb = NewRDBManager(dir, dbfile, server)
	fmt.Printf("RedisServer created with address: %s:%s and RDB info dir: %s file: %s\n", server.address, server.port, dir, dbfile)
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

func (s *MasterServer) Info() map[string]string {
	return map[string]string{
		"role":    s.role,
		"address": s.address,
		"port":    s.port,
	}
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

// Implement the RDBManager interface

func (s *MasterServer) RDBInfo() (string, string) {
	return s.rdb.RDBInfo()
}

func (s *MasterServer) LoadRDBToCache() error {
	return s.rdb.LoadRDBToCache()
}

// Implement the Cache interface

func (s *MasterServer) Set(key, value string) error {
	return s.cache.Set(key, value)
}

func (s *MasterServer) SetExpiry(key, value string, expiry uint64) error {
	return s.cache.SetExpiry(key, value, expiry)
}

func (s *MasterServer) Get(key string) (string, error) {
	return s.cache.Get(key)
}

func (s *MasterServer) Keys(key string) []string {
	return s.cache.Keys(key)
}

func (s *MasterServer) ExpireIn(key string, milliseconds uint64) error {
	return s.cache.ExpireIn(key, milliseconds)
}

func (s *MasterServer) IsExpired(key string) bool {
	return s.cache.IsExpired(key)
}
