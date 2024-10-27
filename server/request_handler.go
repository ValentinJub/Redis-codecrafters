package server

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	rEX = regexp.MustCompile(`^[Ee][Xx]$`) // Set the specified expire time, in seconds.
	rPX = regexp.MustCompile(`^[Pp][Xx]$`) // Set the specified expire time, in milliseconds.
	rNX = regexp.MustCompile(`^[Nn][Xx]$`) // Only set the key if it does not already exist.
	rXX = regexp.MustCompile(`^[Xx][Xx]$`) // Only set the key if it does not already exist.
)

type RequestHandler interface {
	HandleRequest() []byte
}

type ReqHandlerImpl struct {
	request []byte
}

func NewRequestHandler(request []byte) *ReqHandlerImpl {
	return &ReqHandlerImpl{request: request}
}

// Handles a request and returns a response
func (r *ReqHandlerImpl) HandleRequest() []byte {
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
	default:
		return newSimpleString("Unknown command")
	}
}

func (r *ReqHandlerImpl) ping(req *Request) []byte {
	if len(req.args) > 0 {
		return newBulkString(strings.Join(req.args, " "))
	}
	return newSimpleString("PONG")
}

func (r *ReqHandlerImpl) echo(req *Request) []byte {
	return newBulkString(strings.Join(req.args, " "))
}

type SetArgs struct {
	expiry int64 // expiry in milliseconds
	nx     bool  // only set the key if it does not already exist
	xx     bool  // only set the key if it already exists
}

// Extracts the SET command arguments
func (r *ReqHandlerImpl) ExtractSetArgs(args []string) (SetArgs, error) {
	var setArgs SetArgs
	expireSet := false
	for i := 0; i < len(args); i++ {
		if rPX.MatchString(args[i]) {
			if expireSet {
				return setArgs, fmt.Errorf("expiry is already set")
			}
			if i+1 >= len(args) {
				return setArgs, fmt.Errorf("PX requires a value")
			}
			px, err := strconv.Atoi(args[i+1])
			if err != nil {
				return setArgs, err
			}
			setArgs.expiry = int64(px)
			expireSet = true
		} else if rEX.MatchString(args[i]) {
			if expireSet {
				return setArgs, fmt.Errorf("expiry is already set")
			}
			if i+1 >= len(args) {
				return setArgs, fmt.Errorf("EX requires a value")
			}
			px, err := strconv.Atoi(args[i+1])
			if err != nil {
				return setArgs, err
			}
			setArgs.expiry = int64(px) * 1000 // convert to milliseconds
			expireSet = true
		} else if rNX.MatchString(args[i]) {
			setArgs.nx = true
		} else if rXX.MatchString(args[i]) {
			setArgs.xx = true
		}

	}
	return setArgs, nil
}
