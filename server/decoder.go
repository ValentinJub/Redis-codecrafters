package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Decoder interface {
	Decode([]byte) (interface{}, error)
}

// Read 8 bytes from the buffer and convert it to uint64
func buffToUInt64(buffer []byte) uint64 {
	var n uint64
	if err := binary.Read(bytes.NewReader(buffer), binary.BigEndian, &n); err != nil {
		fmt.Println(err)
		return 0
	}
	return n
}

// Read 4 bytes from the buffer and convert it to uint32
func buffToUInt32(buffer []byte) uint32 {
	var n uint32
	if err := binary.Read(bytes.NewReader(buffer), binary.BigEndian, &n); err != nil {
		fmt.Println(err)
		return 0
	}
	return n
}

// Read 2 bytes from the buffer and convert it to uint16
func buffToUInt16(buffer []byte) uint16 {
	var n uint16
	if err := binary.Read(bytes.NewReader(buffer), binary.BigEndian, &n); err != nil {
		fmt.Println(err)
		return 0
	}
	return n
}

// Read 1 byte from the buffer and convert it to uint8
func buffToUInt8(buffer []byte) uint8 {
	var n uint8
	if err := binary.Read(bytes.NewReader(buffer), binary.BigEndian, &n); err != nil {
		fmt.Println(err)
		return 0
	}
	return n
}
