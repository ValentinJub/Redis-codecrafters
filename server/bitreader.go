package server

import (
	"fmt"
)

type BitReader struct {
	buffer []byte
	pos    int // Current bit position in the buffer
}

// NewBitReader creates a new BitReader for a given byte slice
func NewBitReader(data []byte) *BitReader {
	return &BitReader{
		buffer: data,
		pos:    0,
	}
}

// ReadBits reads `bitCount` number of bits from the buffer and returns the value
func (br *BitReader) ReadBits(bitCount int) uint32 {
	if bitCount > 32 {
		fmt.Println("cannot read more than 32 bits at a time")
		return 0
	}

	var value uint32
	remainingBits := bitCount

	for remainingBits > 0 {
		bytePos := br.pos / 8
		bitPos := br.pos % 8
		bitsInCurrentByte := 8 - bitPos

		// Calculate how many bits we can read from the current byte
		bitsToRead := remainingBits
		if bitsToRead > bitsInCurrentByte {
			bitsToRead = bitsInCurrentByte
		}

		// Extract the bits from the current byte
		mask := uint32((1 << bitsToRead) - 1)
		shiftedBits := (uint32(br.buffer[bytePos]) >> (8 - bitPos - bitsToRead)) & mask

		// Shift the result into the correct position in the value
		value = (value << bitsToRead) | shiftedBits

		// Update bit position and remaining bit count
		br.pos += bitsToRead
		remainingBits -= bitsToRead
	}

	return value
}
