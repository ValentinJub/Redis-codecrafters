package server

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
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

	// This properly handles multiple requests received at once
	answerQueue := make([][]byte, 0)
	if len(reqs) > 1 {
		fmt.Printf("Multiple requests received\n")
		for x, req := range reqs {
			fmt.Printf("Decoded request %d: command: %s, args: %v\n", x, req.command, req.args)
			reqHandler := NewReqHandlerMaster(req.Encode(), r.master, r.conn)
			resp := reqHandler.HandleRequest()
			answerQueue = append(answerQueue, resp)
		}
		bigResp := make([]string, 0)
		for _, resp := range answerQueue {
			bigResp = append(bigResp, string(resp))
		}
		r.master.SendTo(r.conn, newBulkArrayOfArrays(bigResp...))
		return []byte{}
	}

	// This loop is uselss since we are returning a response per request, any request after the first one won't be processed, leaving as is for now
	// The loop above fixes that by processing all requests individually and gathering the responses
	for _, req := range reqs {

		fmt.Printf("Decoded request: command: %s, args: %v\n", req.command, req.args)
		// Check if the request needs to be queued, if so, add it to the queue and return QUEUED
		// Do not queue EXEC commands
		if r.master.IsInQueue(r.conn.RemoteAddr().String()) && req.command != "EXEC" {
			r.master.AddToQueue(r.conn.RemoteAddr().String(), req)
			return newSimpleString("QUEUED")
		}

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
				return newSimpleError("ERR " + err.Error())
			}
			go r.master.Propagate(&req)
			r.master.AddAckOffset(commandLen)
			r.master.CacheRequest(&req)
			fmt.Printf("Added %d bytes to Master offset, offset: %d\n", commandLen, r.master.GetAckOffset())
			return resp
		case "INCR":
			if len(req.args) < 1 {
				return newSimpleError("ERR INCR command requires at least 1 argument")
			}
			newValue, err := r.master.Increment(req.args[0])
			if err != nil {
				return newSimpleError("ERR value is not an integer or out of range")
			}
			go r.master.Propagate(&req)
			r.master.AddAckOffset(commandLen)
			r.master.CacheRequest(&req)
			fmt.Printf("Added %d bytes to Master offset, offset: %d\n", commandLen, r.master.GetAckOffset())
			return newInteger(newValue)
		case "GET":
			return r.get(&req)
		case "XRANGE":
			entries, err := r.master.XRange(&req)
			if err != nil {
				return newSimpleError(err.Error())
			}
			return encodeXRangeResponse(entries)
		case "XREAD":
			args, err := r.XReadArgParser(req.args)
			if err != nil {
				return newSimpleError(err.Error())
			}
			xreadEntries, err := r.master.XRead(args)
			if err != nil {
				return newSimpleError(err.Error())
			} else if len(xreadEntries) == 0 {
				return newBulkString("")
			}
			return encodeXReadResponse(args.keys, xreadEntries)
		case "MULTI":
			r.master.Multi(r.conn.RemoteAddr().String())
			return newSimpleString("OK")
		case "EXEC":
			r.exec() // exec is self sufficient, it sends the response to the client
			return []byte{}
		case "CONFIG":
			return r.config(&req)
		case "KEYS":
			return r.keys(&req)
		case "INFO":
			return r.info(&req)
		case "REPLCONF":
			return r.replicationConfig(&req)
		case "PSYNC":
			return r.psync(&req)
		case "WAIT":
			return r.master.Wait(&req)
		case "TYPE":
			if len(req.args) < 1 {
				return newSimpleError("ERR TYPE command requires at least 1 argument")
			}
			return newSimpleString(r.master.Type(req.args[0]))
		default:
			return newSimpleString("Unknown command")
		}
	}
	return []byte{}
}

func (r *ReqHandlerMaster) exec() {
	if r.master.IsInQueue(r.conn.RemoteAddr().String()) {
		reqs := r.master.GetQueuedRequests(r.conn.RemoteAddr().String())
		if len(reqs) == 0 {
			r.master.SendTo(r.conn, newBulkArray([]string{}...))
			r.master.RemoveFromQueue(r.conn.RemoteAddr().String())
			return
		}
		bigReq := make([]byte, 0)
		for _, req := range reqs {
			bigReq = append(bigReq, req.Encode()...)
		}
		r.master.RemoveFromQueue(r.conn.RemoteAddr().String())
		reqHandler := NewReqHandlerMaster(bigReq, r.master, r.conn)
		resp := reqHandler.HandleRequest()
		if len(resp) > 0 {
			go r.master.SendTo(r.conn, resp)
		}
		// for _, req := range reqs {
		// 	reqHandler := NewReqHandlerMaster(req.Encode(), r.master, r.conn)
		// 	resp := reqHandler.HandleRequest()
		// 	go r.master.SendTo(r.conn, resp)
		// }
	} else {
		r.master.SendTo(r.conn, newSimpleError("ERR EXEC without MULTI"))
	}
}

func (r *ReqHandlerMaster) replicationConfig(req *Request) []byte {
	if len(req.args) < 2 {
		return newSimpleError("ERR REPLCONF command requires at least 2 arguments")
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
		return newSimpleError("ERR PSYNC command requires at least 2 arguments")
	}
	infos := r.master.Info()
	go r.master.SendRDBFile(r.conn)
	return newBulkString("+FULLRESYNC " + infos["replicationID"] + " 0")
}

type XReadArg struct {
	keys    []string
	ids     []int
	blockMs int
	lock    bool
}

func newXReadArg() XReadArg {
	return XReadArg{
		keys:    make([]string, 0),
		ids:     make([]int, 0),
		blockMs: 0,
		lock:    false,
	}
}

func (r *ReqHandlerMaster) XReadArgParser(args []string) (XReadArg, error) {
	blockRegexp := regexp.MustCompile(`(?i)^BLOCK$`)
	regexStream := regexp.MustCompile(`(?i)^STREAMS$`)
	regexID := regexp.MustCompile(`^\d+-?\d*$`)
	isStream := false
	if len(args) < 3 {
		return XReadArg{}, fmt.Errorf("XREAD command requires at least 3 arguments")
	}
	argsParsed := newXReadArg()
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "$" { // Akin to using the lastID entry in the stream
			argsParsed.ids = append(argsParsed.ids, -1)
			keyIndex := len(argsParsed.ids) - 1
			if keyIndex < 0 {
				return XReadArg{}, fmt.Errorf("XREAD command requires a key before the $ argument")
			}
			// Get the last entry from the stream
			entry, err := r.master.GetLastEntryFromStream(argsParsed.keys[keyIndex])
			if err != nil {
				return XReadArg{}, err
			}
			if entry.IsEmpty() { // If the stream is empty, set the ID to 0 to effectively read from the beginning
				argsParsed.ids[keyIndex] = 0
				continue
			}
			id, _ := strconv.Atoi(strings.ReplaceAll(entry.ID(), "-", ""))
			argsParsed.ids[keyIndex] = id + 1
		} else if blockRegexp.MatchString(arg) {
			if i+1 >= len(args) {
				return XReadArg{}, fmt.Errorf("XREAD block argument requires a timestamp")
			}
			blockMs, err := strconv.Atoi(args[i+1])
			if err != nil {
				return XReadArg{}, err
			}
			argsParsed.blockMs = blockMs
			argsParsed.lock = true
			i++
		} else if regexStream.MatchString(arg) {
			isStream = true
		} else if regexID.MatchString(arg) {
			id, err := strconv.Atoi(strings.ReplaceAll(arg, "-", ""))
			if err != nil {
				return XReadArg{}, err
			}
			argsParsed.ids = append(argsParsed.ids, id+1)
		} else {
			if isStream {
				argsParsed.keys = append(argsParsed.keys, arg)
			}
		}
	}
	if !isStream {
		return XReadArg{}, fmt.Errorf("XREAD command requires the STREAMS argument to be passed")
	} else if len(argsParsed.keys) != len(argsParsed.ids) {
		return XReadArg{}, fmt.Errorf("XREAD command requires the same number of keys and IDs")
	}
	return argsParsed, nil
}

func encodeXRangeResponse(entries []StreamEntry) []byte {
	content := make([]string, 0)
	for _, entry := range entries {
		id, keyvalue := entry.Values()
		inner := string(newBulkArray(keyvalue...))
		entryID := string(newBulkString(id))
		subArray := string(newBulkArrayOfArrays(entryID, inner))
		content = append(content, subArray)
	}
	resp := newBulkArrayOfArrays(content...)
	fmt.Printf("XRANGE: '%s'\n", strings.ReplaceAll(string(resp), "\r\n", "\\r\\n"))
	return resp
}

func encodeXReadResponse(keyOrder []string, entryMap map[string][]StreamEntry) []byte {
	keys := make([]string, 0)
	for _, key := range keyOrder {
		entries, ok := entryMap[key]
		if !ok {
			fmt.Printf("Key %s not found in entries\n", key)
			continue
		}
		content := make([]string, 0)
		for _, entry := range entries {
			id, keyvalue := entry.Values()
			inner := string(newBulkArray(keyvalue...))
			entryID := string(newBulkString(id))
			subArray := string(newBulkArrayOfArrays(entryID, inner))
			content = append(content, subArray)
		}
		keyContent := string(newBulkArrayOfArrays(content...))
		keyName := string(newBulkString(key))
		keys = append(keys, string(newBulkArrayOfArrays(keyName, keyContent)))
	}
	resp := newBulkArrayOfArrays(keys...)
	fmt.Printf("XREAD: '%s'\n", strings.ReplaceAll(string(resp), "\r\n", "\\r\\n"))
	return resp
}
