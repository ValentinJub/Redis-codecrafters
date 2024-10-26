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

type ReqHandler struct {
	request []byte
	server  RedisServer
}

func NewRequestHandler(request []byte, s RedisServer) *ReqHandler {
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
	case "CONFIG":
		return r.config(req)
	case "KEYS":
		return r.keys(req)
	case "INFO":
		return r.info(req)
	case "REPLCONF":
		r.replicationConfig(req)
		return newSimpleString("OK")
	default:
		return newSimpleString("Unknown command")
	}
}

func (r *ReqHandler) replicationConfig(req *Request) {
	if len(req.args) < 2 {
		return
	}
	for x, arg := range req.args {
		if arg == "listening-port" {
			r.server.AddReplica("127.0.0.1:" + req.args[x+1])
		}
	}
}

func (r *ReqHandler) info(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: INFO command requires at least 1 argument")
	}
	arg := req.args[0]
	header := "# " + arg
	infos := r.server.Info()
	role := fmt.Sprintf("role:%s", infos["role"])
	replID := fmt.Sprintf("%s_replid:%s", infos["role"], infos["replicationID"])
	replOffset := fmt.Sprintf("%s_repl_offset:%s", infos["role"], infos["replicationOffset"])
	return newBulkString(
		header + "\n" + role + "\n" + replID + "\n" + replOffset + "\n",
	)
}

func (r *ReqHandler) keys(req *Request) []byte {
	// Dangerous, need to make sure args[0] is not empty and that no more keys follow
	keys := r.server.Keys(req.args[0])
	return newBulkArray(keys...)
}

func (r *ReqHandler) config(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: CONFIG command requires at least 1 argument (GET or SET)")
	} else if req.args[0] == "GET" {
		return r.configGet(req.args[1])
	}
	return []byte{0}
}

func (r *ReqHandler) configGet(key string) []byte {
	dir, fn := r.server.RDBInfo()
	switch key {
	case "dir":
		return newBulkArray("dir", dir)
	case "dbfilename":
		return newBulkArray("dbfilename", fn)
	default:
		return newBulkString("")
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
	expiry int64 // expiry in milliseconds
	nx     bool  // only set the key if it does not already exist
	xx     bool  // only set the key if it already exists
}

// Extracts the SET command arguments
func extractSetArgs(args []string) (SetArgs, error) {
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

func (r *ReqHandler) set(req *Request) []byte {
	if len(req.args) < 2 {
		return newSimpleString("Error: SET command requires at least 2 arguments")
	}
	args, err := extractSetArgs(req.args)
	if err != nil {
		return newSimpleString(fmt.Sprintf("Error: %s", err))
	}
	if args.nx {
		if _, ok := r.server.Get(req.args[0]); ok == nil {
			return newBulkString("")
		}
	} else if args.xx {
		if _, ok := r.server.Get(req.args[0]); ok != nil {
			return newSimpleString("")
		}
	}
	r.server.Set(req.args[0], req.args[1])
	if args.expiry > 0 {
		r.server.ExpireIn(req.args[0], uint64(args.expiry))
	}

	return newSimpleString("OK")
}

func (r *ReqHandler) get(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: GET command requires at least 1 argument")
	}
	v, _ := r.server.Get(req.args[0])
	return newBulkString(v)
}
