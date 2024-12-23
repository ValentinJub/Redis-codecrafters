package server

import (
	"fmt"
	"strconv"
	"strings"
)

type ReqHandlerMasterReplica struct {
	ReqHandlerImpl
	replica ReplicaServer
}

func NewReqHandlerMasterReplica(request []byte, s ReplicaServer) *ReqHandlerMasterReplica {
	return &ReqHandlerMasterReplica{ReqHandlerImpl: ReqHandlerImpl{request: request, server: s}, replica: s}
}

// Handles a requests silently, doesn not return a response
func (r *ReqHandlerMasterReplica) HandleRequest() {
	re := strings.Split(string(r.request), CRLF)
	if len(re) > 1 && len(re[1]) > 5 && re[1][:5] == "REDIS" {
		fmt.Println("Ignoring Redis RDB")
		return
	}
	fmt.Printf("Master server request: %v\n", re)
	reqd := NewRequestDecoder(re)
	reqs, err := reqd.Decode()
	if err != nil {
		fmt.Printf("error while decoding the requests: %s", err)
	}

	for _, req := range reqs {
		commandLen := len(newBulkArray(append([]string{req.command}, req.args...)...))
		switch req.command {
		case "PING":
		case "DEL":
			r.replica.Del(req.args)
		case "XADD":
			id, err := r.replica.XAdd(&req)
			if err != nil {
				fmt.Printf("Error setting XADD on MasterReplica Handler: " + err.Error())
			} else {
				fmt.Printf("XADD ID: %s added to replica\n", id)
			}
		case "SET":
			_, err := r.set(&req)
			if err != nil {
				fmt.Printf("Error: " + err.Error())
			}
		case "INCR":
			// Must check for length of args
			if len(req.args) < 1 {
				fmt.Printf("INCR command requires at least 1 argument")
				continue
			}
			_, err := r.replica.Increment(req.args[0])
			if err != nil {
				fmt.Printf("Error: " + err.Error())
			}
			fmt.Printf("Added %d bytes to Replica offset, offset: %d\n", commandLen, r.replica.GetAckOffset())
		case "REPLCONF":
			err := r.replicationConfig(&req)
			if err != nil {
				fmt.Printf("Error: " + err.Error())
			}
		default:
			fmt.Printf("Unknown command: %s\n", req.command)
		}
		r.replica.AddAckOffset(commandLen)
		fmt.Printf("Added %d bytes to Replica offset, offset: %d\n", commandLen, r.replica.GetAckOffset())
	}
}

func (r *ReqHandlerMasterReplica) replicationConfig(req *Request) error {
	if len(req.args) < 2 {
		return fmt.Errorf("REPLCONF command requires at least 2 arguments")
	}
	if req.args[0] == "GETACK" {
		offset := r.replica.GetAckOffset()
		err := r.replica.SendToMaster(newBulkArray("REPLCONF", "ACK", strconv.Itoa(offset)))
		if err != nil {
			return fmt.Errorf("error sending ACK to master: %s", err)
		}
	}
	return nil
}
