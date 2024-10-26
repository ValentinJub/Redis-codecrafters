package server

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	utils "github.com/codecrafters-io/redis-starter-go/utils"
)

type RedisServer interface {
	// Initialise the server creating a TCP listener
	Init()
	// Returns various information about the server
	Info() map[string]string
	// Listen for TCP connections using our TCP listener.
	// Encapsulates the request handling process
	Listen()
	// Add a slave to the server
	AddReplica(replica net.Conn)
	Propagate(req *Request)
	// GetReplicas() map[string]bool
	SendRDBFile(conn net.Conn) error
	RDBManager
	Cache
}

const (
	SERVER_ADDR = "127.0.0.1"
	SERVER_PORT = "6379"
)

type Server struct {
	role              string
	address           string
	port              string
	listener          net.Listener
	rdb               RDBManager
	cache             Cache
	replicationID     string
	replicationOffset int
	replicas          map[net.Conn]bool
}

func (s *Server) AddReplica(r net.Conn) {
	if s.role == "master" {
		s.replicas[r] = true
	}
}

func (s *Server) GetReplicas() map[net.Conn]bool {
	return s.replicas
}

func (s *Server) Propagate(req *Request) {
	if s.role == "master" {
		for replica := range s.replicas {
			_, err := replica.Write(newBulkArray(append([]string{req.command}, req.args...)...))
			if err != nil {
				fmt.Printf("Error writing to replica: %s\n", err)
			}
		}
	}
}

// Send an RDB file to a Replica
func (s *Server) SendRDBFile(conn net.Conn) error {
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

// Initialise the server, creating a listener
func (s *Server) Init() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.address, s.port))
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", s.port)
		os.Exit(1)
	}
	s.listener = l
}

func (s *Server) Info() map[string]string {
	return map[string]string{
		"role":              s.role,
		"address":           s.address,
		"port":              s.port,
		"replicationID":     s.replicationID,
		"replicationOffset": strconv.Itoa(s.replicationOffset),
	}
}

// Event loop, handles requests inside it
func (s *Server) Listen() {
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

func (s *Server) RDBInfo() (string, string) {
	return s.rdb.RDBInfo()
}

func (s *Server) LoadRDBToCache() error {
	return s.rdb.LoadRDBToCache()
}

// Implement the Cache interface

func (s *Server) Set(key, value string) error {
	return s.cache.Set(key, value)
}

func (s *Server) SetExpiry(key, value string, expiry uint64) error {
	return s.cache.SetExpiry(key, value, expiry)
}

func (s *Server) Get(key string) (string, error) {
	return s.cache.Get(key)
}

func (s *Server) Keys(key string) []string {
	return s.cache.Keys(key)
}

func (s *Server) ExpireIn(key string, milliseconds uint64) error {
	return s.cache.ExpireIn(key, milliseconds)
}

func (s *Server) IsExpired(key string) bool {
	return s.cache.IsExpired(key)
}

// The ID can be any pseudo random alphanumeric string of 40 characters.
func createReplicationID() string {
	// Create a function that generates a random ID made of alphanumeric characters of length 40
	id, err := GenerateRandomString(40)
	if err != nil {
		return ""
	}
	return id
}

// GenerateRandomString generates a random alphanumeric string of specified length
func GenerateRandomString(n int) (string, error) {
	// Generate random bytes, enough to be encoded as a base64 string of at least length n
	byteLen := (n*6 + 7) / 8
	bytes := make([]byte, byteLen)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	// Encode bytes to base64 and strip any non-alphanumeric characters if necessary
	str := base64.URLEncoding.EncodeToString(bytes)
	str = strings.ReplaceAll(str, "-", "1")
	str = strings.ReplaceAll(str, "_", "2")
	return str[:n], nil
}
