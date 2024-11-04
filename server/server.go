package server

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
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
	XAdd(*Request) (string, error)
	XRange(*Request) ([]StreamEntry, error)
	XRead(XReadArg) (map[string][]StreamEntry, error)
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

func (s *RedisServerImpl) GetStream(key string, start, end int) ([]StreamEntry, error) {
	return s.cache.GetStream(key, start, end)
}

func (s *RedisServerImpl) XRange(req *Request) ([]StreamEntry, error) {
	if len(req.args) < 3 {
		return nil, fmt.Errorf("XRANGE command requires at least 3 arguments")
	}
	key := req.args[0]
	err := error(nil)
	startID, endID := 0, 0
	if req.args[1] != "-" {
		startID, err = strconv.Atoi(strings.ReplaceAll(req.args[1], "-", ""))
		if err != nil {
			return nil, err
		}
	} else {
		startID = 0
	}
	if req.args[2] != "+" {
		endID, err = strconv.Atoi(strings.ReplaceAll(req.args[2], "-", ""))
		if err != nil {
			return nil, fmt.Errorf("error parsing end ID")
		}
	} else {
		endID = int(^uint(0) >> 1) // largest int
	}
	return s.GetStream(key, startID, endID)
}

func (s *RedisServerImpl) XRead(args XReadArg) (map[string][]StreamEntry, error) {
	entriesMap := make(map[string][]StreamEntry)
	if args.lock {
		now := time.Now().UnixMilli()
		var endTime int64
		if args.blockMs == 0 { // Lock the transaction until a new entry is added
			endTime = int64(^uint(0) >> 1)
		} else {
			endTime = now + int64(args.blockMs)
		}
		for now < endTime {
			for x, key := range args.keys {
				entries, err := s.GetStream(key, args.ids[x], int(^uint(0)>>1))
				if err != nil {
					return nil, err
				}
				if len(entries) > 0 {
					entriesMap[key] = entries
					return entriesMap, nil
				}
			}
			time.Sleep(5 * time.Millisecond)
			now = time.Now().UnixMilli()
		}
	} else {
		for x, key := range args.keys {
			entries, err := s.GetStream(key, args.ids[x], int(^uint(0)>>1))
			if err != nil {
				return nil, err
			}
			entriesMap[key] = entries
		}
	}
	return entriesMap, nil
}
