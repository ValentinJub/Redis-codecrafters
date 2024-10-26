package server

import (
	"crypto/rand"
	"encoding/base64"
)

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
	return str[:n], nil
}
