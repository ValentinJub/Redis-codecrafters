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
}

func NewRequestHandler(request []byte) *ReqHandler {
	return &ReqHandler{request: request}
}

// Handles a request and returns a response
func (r *ReqHandler) HandleRequest() []byte {
	// The request can be stringifyied
	re := strings.Split(string(r.request), CRLF)
	fmt.Println(re)
	req := NewRequest(re)
	err := req.Decode()
	if err != nil {
		fmt.Printf("error while decoding the request: %s", err)
		return []byte{}
	}

	switch req.command {
	case "PING":
		if len(req.args) > 0 {
			return newBulkString(strings.Join(req.args, " "))
		}
		return newSimpleString("PONG")
	case "ECHO":
		return newBulkString(strings.Join(req.args, " "))
	default:
		return []byte{}
	}
}

func newSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s%s", s, CRLF))
}

func newBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d%s%s%s", len(s), CRLF, s, CRLF))
}
