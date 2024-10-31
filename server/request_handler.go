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
	server  RedisServer
}

func NewRequestHandler(request []byte, s RedisServer) *ReqHandlerImpl {
	return &ReqHandlerImpl{request: request, server: s}
}

// Handles a request and returns a response
func (r *ReqHandlerImpl) HandleRequest() []byte {
	re := strings.Split(string(r.request), CRLF)
	fmt.Printf("Client request: %v\n", re)
	reqD := NewRequestDecoder(re)
	reqs, err := reqD.Decode()
	if err != nil {
		fmt.Printf("error while decoding the request: %s", err)
		return []byte{}
	}

	for _, req := range reqs {
		switch req.command {
		case "PING":
			return r.ping(&req)
		case "ECHO":
			return r.echo(&req)
		case "GET":
			return r.get(&req)
		case "CONFIG":
			return r.config(&req)
		case "KEYS":
			return r.keys(&req)
		case "INFO":
			return r.info(&req)
		case "TYPE":
			if len(req.args) < 1 {
				return newSimpleString("Error: TYPE command requires at least 1 argument")
			}
			return newSimpleString(r.server.Type(req.args[0]))
		default:
			return newSimpleString("Unknown command")
		}
	}
	return []byte{}
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

func (r *ReqHandlerImpl) set(req *Request) ([]byte, error) {
	if len(req.args) < 2 {
		return newSimpleString("error"), fmt.Errorf("error: SET command requires at least 2 arguments")
	}
	args, err := r.ExtractSetArgs(req.args)
	if err != nil {
		return newSimpleString("error"), fmt.Errorf("error: while extracting set args")
	}
	if args.nx {
		if _, ok := r.server.Get(req.args[0]); ok == nil {
			return newBulkString(""), fmt.Errorf("error: key already exists")
		}
	} else if args.xx {
		if _, ok := r.server.Get(req.args[0]); ok != nil {
			return newSimpleString(""), fmt.Errorf("error: key does not exist")
		}
	}
	r.server.Set(req.args[0], req.args[1])
	if args.expiry > 0 {
		r.server.ExpireIn(req.args[0], uint64(args.expiry))
	}

	return newSimpleString("OK"), nil
}
func (r *ReqHandlerImpl) get(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: GET command requires at least 1 argument")
	}
	v, _ := r.server.Get(req.args[0])
	return newBulkString(v)
}

func (r *ReqHandlerImpl) info(req *Request) []byte {
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

func (r *ReqHandlerImpl) keys(req *Request) []byte {
	// Dangerous, need to make sure args[0] is not empty and that no more keys follow
	keys := r.server.Keys(req.args[0])
	return newBulkArray(keys...)
}

func (r *ReqHandlerImpl) config(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: CONFIG command requires at least 1 argument (GET or SET)")
	} else if req.args[0] == "GET" {
		return r.configGet(req.args[1])
	}
	return []byte{0}
}

func (r *ReqHandlerImpl) configGet(key string) []byte {
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
