package server

import (
	"fmt"
	"strconv"
	"strings"
)

type RequestDecoder struct {
	data   []string
	cursor int
}

func NewRequestDecoder(data []string) *RequestDecoder {
	return &RequestDecoder{data: data, cursor: 0}
}

func (r *RequestDecoder) Decode() ([]Request, error) {
	requests := make([]Request, 0)
	for r.cursor < len(r.data)-1 {
		req := NewRequest(r.data[r.cursor:])
		err := req.Decode()
		if err != nil {
			return nil, err
		}
		requests = append(requests, *req)
		// Args and their length + command and its length + parts number
		r.cursor += (len(req.args) * 2) + 2 + 1
	}
	return requests, nil
}

type Request struct {
	data    []string
	command string
	args    []string
}

func NewRequest(data []string) *Request {
	return &Request{data: data, args: make([]string, 0)}
}

func (r *Request) Decode() error {
	fmt.Printf("About to decode: %v\n", r.data)
	partsNumber, err := strconv.Atoi(r.data[0][1:])
	if err != nil {
		return err
	}
	data := r.data[1:]
	for i, j := 0, 1; i < partsNumber && j < len(r.data); i, j = i+1, j+2 {
		if i == 0 {
			r.command = strings.ToUpper(data[j])
			continue
		}
		r.args = append(r.args, data[j])
	}
	return nil
}
