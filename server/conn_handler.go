package server

import (
	"errors"
	"fmt"
	"io"
	"net"
)

type ConnectionHandler interface {
	HandleConnection()
}

type ConnHandler struct {
	conn   net.Conn
	server RedisServer
}

func NewConnHandler(conn net.Conn, s RedisServer) *ConnHandler {
	return &ConnHandler{conn: conn, server: s}
}

// Handle incoming TCP Requests
func (c *ConnHandler) HandleConnection() {
	buff := make([]byte, 1024)
	for {
		// Read from the connection
		bytesRead, err := c.conn.Read(buff)
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.conn.Close()
				break
			}
			fmt.Println(err)
			return
		}
		// The data read from the TCP stream
		request := buff[:bytesRead]
		// Handles the decoded request and produce an answer
		reqHandler := NewRequestHandler(request, c.server)
		response := reqHandler.HandleRequest()
		_, err = c.conn.Write(response)
		if err != nil {
			fmt.Println(err)
			break
		}
	}
}
