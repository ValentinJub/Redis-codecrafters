package server

import (
	"fmt"
	"strings"
)

type RequestHandler interface {
	HandleRequest() []byte
}

type ReqHandler struct {
	request []byte
	server  *MasterServer
}

func NewRequestHandler(request []byte, s *MasterServer) *ReqHandler {
	return &ReqHandler{request: request, server: s}
}

// Handles a request and returns a response
func (r *ReqHandler) HandleRequest() []byte {
	// The request can be stringifyied
	re := strings.Split(string(r.request), CRLF)
	fmt.Printf("Client request: %v\n", re)
	req := NewRequest(re)
	err := req.Decode()
	if err != nil {
		fmt.Printf("error while decoding the request: %s", err)
		return []byte{}
	}

	switch req.command {
	case "PING":
		return r.ping(req)
	case "ECHO":
		return r.echo(req)
	case "SET":
		return r.set(req)
	case "GET":
		return r.get(req)
	default:
		return newSimpleString("Unknown command")
	}
}

func (r *ReqHandler) ping(req *Request) []byte {
	if len(req.args) > 0 {
		return newBulkString(strings.Join(req.args, " "))
	}
	return newSimpleString("PONG")
}

func (r *ReqHandler) echo(req *Request) []byte {
	return newBulkString(strings.Join(req.args, " "))
}

func (r *ReqHandler) set(req *Request) []byte {
	if len(req.args) < 2 {
		return newSimpleString("Error: SET command requires at least 2 arguments")
	}
	r.server.Set(req.args[0], req.args[1])
	return newSimpleString("OK")
}

func (r *ReqHandler) get(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: GET command requires at least 1 argument")
	}
	return newBulkString(r.server.Get(req.args[0]))
}

func newSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s%s", s, CRLF))
}

func newBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d%s%s%s", len(s), CRLF, s, CRLF))
}
