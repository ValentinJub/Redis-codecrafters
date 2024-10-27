package server

import (
	"fmt"
	"strings"
)

type ReqHanderMasterReplica struct {
	ReqHanderReplica
}

func NewReqHandlerMasterReplica(request []byte, s ReplicaServer) *ReqHanderMasterReplica {
	return &ReqHanderMasterReplica{ReqHanderReplica{ReqHandlerImpl: ReqHandlerImpl{request: request}, replica: s}}
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
		switch req.command {
		case "SET":
			_, err := r.set(&req)
			if err != nil {
				fmt.Printf("Error: " + err.Error())
			}
		default:
			fmt.Printf("Unknown command: %s\n", req.command)
		}
	}
}
