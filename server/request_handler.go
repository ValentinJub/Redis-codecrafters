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
	req := strings.Split(string(r.request), CRLF)
	fmt.Println(req)
	if len(req) < 1 {
		fmt.Printf("Error: received a request with less than one elements: %v", req)
		return []byte{}
	}

	return []byte("+PONG\r\n")
}
