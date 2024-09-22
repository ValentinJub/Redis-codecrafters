package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

const CRLF = "\r\n"

type request []byte

func (r request) String() {
	for x, c := range r {
		fmt.Printf("'%s'", string(c))
		if x != len(r)-1 {
			fmt.Printf(", ")
		}
	}
}

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

		request := request(buff[:bytesRead])
		response := handleRequest(request)
		_, err = conn.Write(response)
		if err != nil {
			fmt.Println(err)
			break
		}
	}
}

// Handles a request and returns a response
func handleRequest(r request) []byte {
	// The request can be stringifyied
	req := strings.Split(string(r), CRLF)
	fmt.Println(req)
	if len(req) < 1 {
		fmt.Printf("Error: received a request with less than one elements: %v", req)
		return []byte{}
	}

	return []byte("+PONG\r\n")
}
