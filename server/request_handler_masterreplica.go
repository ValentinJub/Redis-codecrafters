package server

import (
	"fmt"
	"strconv"
	"strings"
)

type ReqHanderMasterReplica struct {
	ReqHandlerReplica
}

func NewReqHandlerMasterReplica(request []byte, s ReplicaServer) *ReqHanderMasterReplica {
	return &ReqHanderMasterReplica{ReqHandlerReplica{ReqHandlerImpl: ReqHandlerImpl{request: request}, replica: s}}
}

// Handles a requests silently, doesn not return a response
func (r *ReqHanderMasterReplica) HandleRequest() {
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
		len := len(newBulkArray(append([]string{req.command}, req.args...)...))
		switch req.command {
		case "PING":
		case "SET":
			_, err := r.set(&req)
			if err != nil {
				fmt.Printf("Error: " + err.Error())
			}
		case "REPLCONF":
			err := r.replicationConfig(&req)
			if err != nil {
				fmt.Printf("Error: " + err.Error())
			}
		default:
			fmt.Printf("Unknown command: %s\n", req.command)
		}
		r.replica.AddAckOffset(len)
		fmt.Printf("Added %d bytes to offset, offset: %d\n", len, r.replica.GetAckOffset())
	}
}
func (r *ReqHandlerReplica) replicationConfig(req *Request) error {
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
