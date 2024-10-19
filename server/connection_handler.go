package server

import (
	"fmt"
	"io"
	"net"
)

type ConnectionHandler interface {
	HandleConnection()
}

type ConnHandler struct {
	conn net.Conn
}

func NewConnHandler(conn net.Conn) *ConnHandler {
	return &ConnHandler{conn: conn}
}

func (c *ConnHandler) HandleConnection() {
	buff := make([]byte, 1024)
	for {
		bytesRead, err := c.conn.Read(buff)
		if err != nil && err != io.EOF {
			fmt.Println(err)
			return
		}
		request := buff[:bytesRead]
		reqHandler := NewRequestHandler(request)
		response := reqHandler.HandleRequest()
		_, err = c.conn.Write(response)
		if err != nil {
			fmt.Println(err)
			break
		}
	}
}
