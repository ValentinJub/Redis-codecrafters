package server

import (
	"fmt"
	"net"
	"strings"
)

type ReqHandlerMaster struct {
	ReqHandlerImpl
	master MasterServer
	conn   net.Conn
}

func NewReqHandlerMaster(request []byte, s MasterServer, c net.Conn) *ReqHandlerMaster {
	return &ReqHandlerMaster{ReqHandlerImpl: ReqHandlerImpl{request: request}, master: s, conn: c}
}

// Handles a request and returns a response
func (r *ReqHandlerMaster) HandleRequest() []byte {
	// The request can be stringifyied
	re := strings.Split(string(r.request), CRLF)
	fmt.Printf("Client request: %v\n", re)
	reqD := NewRequestDecoder(re)
	reqs, err := reqD.Decode()
	if err != nil {
		fmt.Printf("error while decoding the request: %s", err)
		return []byte{}
	}

	for _, req := range reqs {
		len := len(newBulkArray(append([]string{req.command}, req.args...)...))
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
			go r.master.Propagate(&req)
			r.master.AddAckOffset(len)
			r.master.CacheRequest(&req)
			fmt.Printf("Added %d bytes to Master offset, offset: %d\n", len, r.master.GetAckOffset())
			return resp
		case "GET":
			return r.get(&req)
		case "CONFIG":
			return r.config(&req)
		case "KEYS":
			return r.keys(&req)
		case "INFO":
			return r.info(&req)
		case "REPLCONF":
			return r.replicationConfig(&req)
		case "PSYNC":
			go r.master.SendRDBFile(r.conn)
			return r.psync(&req)
		case "WAIT":
			return r.master.Wait(&req)
		default:
			return newSimpleString("Unknown command")
		}
	}
	return []byte{}
}

func (r *ReqHandlerMaster) replicationConfig(req *Request) []byte {
	if len(req.args) < 2 {
		return newSimpleString("Error: REPLCONF command requires at least 2 arguments")
	}
	for _, arg := range req.args {
		switch arg {
		case "listening-port":
			addr := r.conn.RemoteAddr().String()
			r.master.AddReplica(addr, r.conn)
		case "ACK":
			fmt.Printf("Received ACK from replica %s\n", r.conn.RemoteAddr().String())
			r.master.AddAckReceived()
			return []byte{}
		}

	}
	return newSimpleString("OK")
}

func (r *ReqHandlerMaster) psync(req *Request) []byte {
	if len(req.args) < 2 {
		return newSimpleString("Error: PSYNC command requires at least 2 arguments")
	}
	infos := r.master.Info()
	return newBulkString("+FULLRESYNC " + infos["replicationID"] + " 0")
}

func (r *ReqHandlerMaster) info(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: INFO command requires at least 1 argument")
	}
	arg := req.args[0]
	header := "# " + arg
	infos := r.master.Info()
	role := fmt.Sprintf("role:%s", infos["role"])
	replID := fmt.Sprintf("%s_replid:%s", infos["role"], infos["replicationID"])
	replOffset := fmt.Sprintf("%s_repl_offset:%s", infos["role"], infos["replicationOffset"])
	return newBulkString(
		header + "\n" + role + "\n" + replID + "\n" + replOffset + "\n",
	)
}

func (r *ReqHandlerMaster) keys(req *Request) []byte {
	// Dangerous, need to make sure args[0] is not empty and that no more keys follow
	keys := r.master.Keys(req.args[0])
	return newBulkArray(keys...)
}

func (r *ReqHandlerMaster) config(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: CONFIG command requires at least 1 argument (GET or SET)")
	} else if req.args[0] == "GET" {
		return r.configGet(req.args[1])
	}
	return []byte{0}
}

func (r *ReqHandlerMaster) configGet(key string) []byte {
	dir, fn := r.master.RDBInfo()
	switch key {
	case "dir":
		return newBulkArray("dir", dir)
	case "dbfilename":
		return newBulkArray("dbfilename", fn)
	default:
		return newBulkString("")
	}
}

func (r *ReqHandlerMaster) ping(req *Request) []byte {
	if len(req.args) > 0 {
		return newBulkString(strings.Join(req.args, " "))
	}
	return newSimpleString("PONG")
}

func (r *ReqHandlerMaster) echo(req *Request) []byte {
	return newBulkString(strings.Join(req.args, " "))
}

func (r *ReqHandlerMaster) set(req *Request) ([]byte, error) {
	if len(req.args) < 2 {
		return newSimpleString("error"), fmt.Errorf("error: SET command requires at least 2 arguments")
	}
	args, err := r.ExtractSetArgs(req.args)
	if err != nil {
		return newSimpleString("error"), fmt.Errorf("error: while extracting set args")
	}
	if args.nx {
		if _, ok := r.master.Get(req.args[0]); ok == nil {
			return newBulkString(""), fmt.Errorf("error: key already exists")
		}
	} else if args.xx {
		if _, ok := r.master.Get(req.args[0]); ok != nil {
			return newSimpleString(""), fmt.Errorf("error: key does not exist")
		}
	}
	r.master.Set(req.args[0], req.args[1])
	if args.expiry > 0 {
		r.master.ExpireIn(req.args[0], uint64(args.expiry))
	}

	return newSimpleString("OK"), nil
}

func (r *ReqHandlerMaster) get(req *Request) []byte {
	if len(req.args) < 1 {
		return newSimpleString("Error: GET command requires at least 1 argument")
	}
	v, _ := r.master.Get(req.args[0])
	return newBulkString(v)
}
