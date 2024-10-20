package server

import (
	"strconv"
	"strings"
)

type Request struct {
	data    []string
	command string
	args    []string
}

func NewRequest(data []string) *Request {
	return &Request{data: data, args: make([]string, 0)}
}

func (r *Request) Decode() error {
	// fmt.Printf("About to decode: %v\n", r.data)
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
