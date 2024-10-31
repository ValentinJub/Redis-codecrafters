package server

import (
	"fmt"
	"strings"
)

type ReqHandlerReplica struct {
	ReqHandlerImpl
	replica ReplicaServer
}

func NewReqHandlerReplica(request []byte, s ReplicaServer) *ReqHandlerReplica {
	return &ReqHandlerReplica{ReqHandlerImpl: ReqHandlerImpl{request: request}, replica: s}
}

// Handles a request and returns a response
func (r *ReqHandlerReplica) HandleRequest() []byte {
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
		case "SET":
			resp, err := r.set(&req)
			if err != nil {
				return newSimpleString("Error: " + err.Error())
			}
			return resp
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
			return newSimpleString(r.replica.Type(req.args[0]))
		default:
			return newSimpleString("Unknown command")
		}
	}
	return []byte{}
}

func (r *ReqHandlerReplica) info(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: INFO command requires at least 1 argument")
	}
	arg := req.args[0]
	header := "# " + arg
	infos := r.replica.Info()
	role := fmt.Sprintf("role:%s", infos["role"])
	replID := fmt.Sprintf("%s_replid:%s", infos["role"], infos["replicationID"])
	replOffset := fmt.Sprintf("%s_repl_offset:%s", infos["role"], infos["replicationOffset"])
	return newBulkString(
		header + "\n" + role + "\n" + replID + "\n" + replOffset + "\n",
	)
}

func (r *ReqHandlerReplica) keys(req *Request) []byte {
	// Dangerous, need to make sure args[0] is not empty and that no more keys follow
	keys := r.replica.Keys(req.args[0])
	return newBulkArray(keys...)
}

func (r *ReqHandlerReplica) config(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: CONFIG command requires at least 1 argument (GET or SET)")
	} else if req.args[0] == "GET" {
		return r.configGet(req.args[1])
	}
	return []byte{0}
}

func (r *ReqHandlerReplica) configGet(key string) []byte {
	dir, fn := r.replica.RDBInfo()
	switch key {
	case "dir":
		return newBulkArray("dir", dir)
	case "dbfilename":
		return newBulkArray("dbfilename", fn)
	default:
		return newBulkString("")
	}
}

func (r *ReqHandlerReplica) ping(req *Request) []byte {
	if len(req.args) > 0 {
		return newBulkString(strings.Join(req.args, " "))
	}
	return newSimpleString("PONG")
}

func (r *ReqHandlerReplica) echo(req *Request) []byte {
	return newBulkString(strings.Join(req.args, " "))
}

func (r *ReqHandlerReplica) set(req *Request) ([]byte, error) {
	if len(req.args) < 2 {
		return newSimpleString("error"), fmt.Errorf("error: SET command requires at least 2 arguments")
	}
	args, err := r.ExtractSetArgs(req.args)
	if err != nil {
		return newSimpleString("error"), fmt.Errorf("error: while extracting set args")
	}
	if args.nx {
		if _, ok := r.replica.Get(req.args[0]); ok == nil {
			return newBulkString(""), fmt.Errorf("error: key already exists")
		}
	} else if args.xx {
		if _, ok := r.replica.Get(req.args[0]); ok != nil {
			return newSimpleString(""), fmt.Errorf("error: key does not exist")
		}
	}
	r.replica.Set(req.args[0], req.args[1])
	if args.expiry > 0 {
		r.replica.ExpireIn(req.args[0], uint64(args.expiry))
	}

	return newSimpleString("OK"), nil
}

func (r *ReqHandlerReplica) get(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: GET command requires at least 1 argument")
	}
	v, _ := r.replica.Get(req.args[0])
	return newBulkString(v)
}
