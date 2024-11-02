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
	return &ReqHandlerMaster{ReqHandlerImpl: ReqHandlerImpl{request: request, server: s}, master: s, conn: c}
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
		commandLen := len(newBulkArray(append([]string{req.command}, req.args...)...))
		switch req.command {
		case "PING":
			return r.ping(&req)
		case "ECHO":
			return r.echo(&req)
		case "XADD":
			resp, err := r.master.XAdd(&req)
			if err != nil {
				return newSimpleError(err.Error())
			}
			go r.master.Propagate(&req)
			r.master.AddAckOffset(commandLen)
			r.master.CacheRequest(&req)
			fmt.Printf("Added %d bytes to Master offset, offset: %d\n", commandLen, r.master.GetAckOffset())
			return newBulkString(resp)
		case "SET":
			resp, err := r.set(&req)
			if err != nil {
				return newSimpleString("Error: " + err.Error())
			}
			go r.master.Propagate(&req)
			r.master.AddAckOffset(commandLen)
			r.master.CacheRequest(&req)
			fmt.Printf("Added %d bytes to Master offset, offset: %d\n", commandLen, r.master.GetAckOffset())
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
		case "TYPE":
			if len(req.args) < 1 {
				return newSimpleString("Error: TYPE command requires at least 1 argument")
			}
			return newSimpleString(r.master.Type(req.args[0]))
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
