package server

import (
	"fmt"
	"strconv"
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

type SetArgs struct {
	px int64
}

func extractSetArgs(args []string) (SetArgs, error) {
	var setArgs SetArgs
	for i := 0; i < len(args); i++ {
		if args[i] == "px" {
			if i+1 >= len(args) {
				return setArgs, fmt.Errorf("PX requires a value")
			}
			px, err := strconv.Atoi(args[i+1])
			if err != nil {
				return setArgs, err
			}
			setArgs.px = int64(px)
		}
	}
	return setArgs, nil
}

func (r *ReqHandler) set(req *Request) []byte {
	if len(req.args) < 2 {
		return newSimpleString("Error: SET command requires at least 2 arguments")
	}
	args, err := extractSetArgs(req.args)
	if err != nil {
		return newSimpleString(fmt.Sprintf("Error: %s", err))
	}
	r.server.cache.Set(req.args[0], req.args[1])
	if args.px > 0 {
		r.server.cache.Expire(req.args[0], args.px)
	}

	return newSimpleString("OK")
}

func (r *ReqHandler) get(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: GET command requires at least 1 argument")
	}
	v, _ := r.server.cache.Get(req.args[0])
	return newBulkString(v)
}

func newSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s%s", s, CRLF))
}

func newBulkString(s string) []byte {
	if s == "" {
		return []byte("$-1" + CRLF)
	}
	return []byte(fmt.Sprintf("$%d%s%s%s", len(s), CRLF, s, CRLF))
}
