package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleClient(conn)
	}
}

// A client has connected to our server
func handleClient(conn net.Conn) {
	// Read from the connection
	buff := make([]byte, 1024)

	for {
		bytesRead, err := conn.Read(buff)
		if err != nil && err != io.EOF {
			fmt.Println(err)
			return
		}

		request := buff[:bytesRead]
		handleRequest(request)
	}
}

// Handles a request and returns a response
func handleRequest(r []byte) []byte {
	// We need to decode the request
	fmt.Println(string(r))
	return []byte{}
}
