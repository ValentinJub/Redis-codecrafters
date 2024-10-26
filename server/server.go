package server

type RedisServer interface {
	// Initialise the server creating a TCP listener
	Init()
	// Listen for TCP connections using our TCP listener.
	// Encapsulates the request handling process
	Listen()
	// Returns various information about the server
	Info() map[string]string
	RDBManager
	Cache
}

const (
	SERVER_ADDR = "127.0.0.1"
	SERVER_PORT = "6379"
)
