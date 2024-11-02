package server

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

type RedisServer interface {
	// Initialise the server creating a TCP listener
	Init()
	// Returns various information about the server
	Info() map[string]string
	// Listen for TCP connections using our TCP listener.
	// Encapsulates the request handling process
	Listen()
	HandleClientConnections(conn net.Conn)
	AddAckOffset(offset int)
	GetAckOffset() int
	XAdd(req *Request) (string, error)
	RDBManager
	Cache
}

const (
	SERVER_ADDR = "127.0.0.1"
	SERVER_PORT = "6379"
)

type RedisServerImpl struct {
	role              string
	address           string
	port              string
	listener          net.Listener
	rdb               RDBManager
	cache             Cache
	replicationID     string
	replicationOffset int
}

func (s *RedisServerImpl) AddAckOffset(offset int) {
	s.replicationOffset += offset
}

// Initialise the server, creating a listener
func (s *RedisServerImpl) Init() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.address, s.port))
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", s.port)
		os.Exit(1)
	}
	s.listener = l
}

func (s *RedisServerImpl) Info() map[string]string {
	return map[string]string{
		"role":              s.role,
		"address":           s.address,
		"port":              s.port,
		"replicationID":     s.replicationID,
		"replicationOffset": strconv.Itoa(s.replicationOffset),
	}
}

func (s *RedisServerImpl) GetAckOffset() int {
	return s.replicationOffset
}

// Implement the RDBManager interface

func (s *RedisServerImpl) RDBInfo() (string, string) {
	return s.rdb.RDBInfo()
}

func (s *RedisServerImpl) LoadRDBToCache() error {
	return s.rdb.LoadRDBToCache()
}

// Implement the Cache interface

func (s *RedisServerImpl) Set(key, value string) error {
	return s.cache.Set(key, value)
}

func (s *RedisServerImpl) SetExpiry(key, value string, expiry uint64) error {
	return s.cache.SetExpiry(key, value, expiry)
}

func (s *RedisServerImpl) SetStream(key, id string, fields map[string]string) (string, error) {
	return s.cache.SetStream(key, id, fields)
}

func (s *RedisServerImpl) Get(key string) (string, error) {
	return s.cache.Get(key)
}

func (s *RedisServerImpl) Keys(key string) []string {
	return s.cache.Keys(key)
}

func (s *RedisServerImpl) ExpireIn(key string, milliseconds uint64) error {
	return s.cache.ExpireIn(key, milliseconds)
}

func (s *RedisServerImpl) IsExpired(key string) bool {
	return s.cache.IsExpired(key)
}

func (s *RedisServerImpl) Type(key string) string {
	return s.cache.Type(key)
}

func (s *RedisServerImpl) XAdd(req *Request) (string, error) {
	if len(req.args) < 4 {
		return "", fmt.Errorf("XADD command requires at least 4 arguments")
	}
	// Extract the stream key and the fields
	key := req.args[0]
	id := req.args[1]
	fields := make(map[string]string)
	for i := 2; i < len(req.args); i += 2 {
		if i+1 >= len(req.args) {
			return "", fmt.Errorf("XADD command requires an even number of arguments")
		}
		fields[req.args[i]] = req.args[i+1]
	}
	newID, err := s.SetStream(key, id, fields)
	if err != nil {
		return "", err
	}
	return newID, nil
}
