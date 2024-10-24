package server

import (
	"fmt"
	"strconv"
)

/*

MUST READ: https://rdb.fnordig.de/file_format.html#redis-rdb-file-format

*/

const (
	// OPCode for RDB file
	OPCodeEOF          = 0xFF
	OPCodeSelectDB     = 0xFE
	OPCodeExpireTime   = 0xFD
	OPCodeExpireTimeMS = 0xFC
	OPCodeResizeDB     = 0xFB
	OPCodeAuxField     = 0xFA
)

var ValueType = []string{
	"String Encoding",                // 0
	"List Encoding",                  // 1
	"Set Encoding",                   // 2
	"Sorted Set Encoding",            // 3
	"Hash Encoding",                  // 4
	"",                               // 5
	"",                               // 6
	"",                               // 7
	"",                               // 8
	"Zipmap Encoding",                // 9
	"Ziplist Encoding",               // 10
	"Intset Encoding",                // 11
	"Sorted Set in Ziplist Encoding", // 12
	"Hashmap in Ziplist Encoding",    // 13
	"List in Quicklist encoding",     // 14
}

// Struct to decode the content of the RDB file
type RDBDecoder struct {
	data   []byte
	offset int
	max    int
}

func NewRDBDecoder(data []byte) *RDBDecoder {
	return &RDBDecoder{data: data, max: len(data)}
}

// Decode the RDB file and return a map of key value pairs and their expiry
func (r *RDBDecoder) Decode() (interface{}, error) {
	header, err := r.decodeHeader()
	if err != nil {
		return nil, err
	}
	fmt.Print(header.String())
	mt, err := r.decodeMetadata()
	if err != nil {
		return nil, err
	}
	fmt.Print(mt.String())
	db, err := r.decodeDatabase()
	if err != nil {
		return nil, err
	}
	fmt.Print(db.String())
	return db.objects, err
}

// Decode the header of the RDB file
func (r *RDBDecoder) decodeHeader() (*RDBHeader, error) {
	magic := r.readMagicString()
	if magic != "REDIS" {
		return nil, fmt.Errorf("error: invalid RDB file")
	}
	version := r.readStringLen(4)
	v, err := strconv.Atoi(version)
	if err != nil {
		return nil, err
	}
	return &RDBHeader{magic: magic, dbversionnum: v}, nil
}

// Decode the metadata of the RDB file
func (r *RDBDecoder) decodeMetadata() (*RDBMetadata, error) {
	metadata := make(map[string]string)
	for {
		if r.peekUInt8() == OPCodeAuxField {
			r.offset++
			key := r.readString()
			value := r.readString()
			metadata[key] = value
		} else {
			break
		}
	}
	return &RDBMetadata{data: metadata}, nil
}

// Decode the database content of the RDB file
func (r *RDBDecoder) decodeDatabase() (*RDBdatabase, error) {
	db := NewRDBDatabase()
	fmt.Printf("Decoding database, starting at pos: %d\n", r.offset)
	for {
		opcode := r.readUInt8()
		fmt.Printf("Opcode: %x\n", opcode)
		switch {
		case opcode == OPCodeSelectDB:
			db.dbIndex = int(r.readUInt8())
			fmt.Printf("Db Index found: %d\n", db.dbIndex)
		case opcode == OPCodeResizeDB:
			fmt.Printf("Offset before num of keys: %d\n", r.offset)
			db.numOfKeys = r.readLength()
			fmt.Printf("Offset before num of expires: %d\n", r.offset)
			db.numOfExpires = r.readLength()
			fmt.Printf("Num of keys: %d, Num of expires: %d\n", db.numOfKeys, db.numOfExpires)
			fmt.Printf("Offset resize DB: %d\n", r.offset)
		case opcode == OPCodeExpireTimeMS:
			obj := Object{}
			obj.expiry = uint64(r.readUInt64())
			vtype := ValueType[r.readUInt8()]
			key := r.readString()
			if vtype == "String Encoding" {
				obj.value = r.readString()
			}
			db.objects[key] = obj
		case opcode == OPCodeExpireTime:
			obj := Object{}
			obj.expiry = uint64(r.readUInt32())
			vtype := ValueType[r.readUInt8()]
			key := r.readString()
			if vtype == "String Encoding" {
				obj.value = r.readString()
			}
			db.objects[key] = obj
		case opcode == OPCodeEOF:
			return db, nil
		default:
			// Decrease the offset by 1 to read the value type
			r.offset--
			fmt.Printf("Unknown opcode: %x, about to decode the value type, offset is: %d\n", opcode, r.offset)
			e := r.getValueType()
			switch e {
			case "String Encoding":
				fmt.Println("A string encoding found in the key value pairs")
				key := r.readString()
				value := r.readString()
				db.objects[key] = Object{value: value}
			default:
				fmt.Printf("Value type not implemented yet: %s\n", e)
				return db, nil
			}
		}
	}
}

/*
A one byte flag indicates encoding used to save the Value.

0 = String Encoding
1 = List Encoding
2 = Set Encoding
3 = Sorted Set Encoding
4 = Hash Encoding
9 = Zipmap Encoding
10 = Ziplist Encoding
11 = Intset Encoding
12 = Sorted Set in Ziplist Encoding
13 = Hashmap in Ziplist Encoding (Introduced in RDB version 4)
14 = List in Quicklist encoding (Introduced in RDB version 7)
*/
func (r *RDBDecoder) getValueType() string {
	typ := r.readUInt8()
	if typ > 14 || typ == 5 || typ == 6 || typ == 7 || typ == 8 {
		fmt.Printf("Invalid value type: %d\n", typ)
		return ""
	}
	fmt.Printf("Value type: %s\n", ValueType[typ])
	return ValueType[typ]
}

// Read the magic string from the RDB file, it's the fist 5 bytes of the file
// Expected value is "REDIS"
func (r *RDBDecoder) readMagicString() string {
	return r.readStringLen(5)
}

// Read a string of a specific length from current offset
func (r *RDBDecoder) readStringLen(length int) string {
	if len(r.data) <= r.offset+length {
		return ""
	}
	s := string(r.data[r.offset : r.offset+length])
	r.offset += length
	return s
}

// Peek the next byte in the RDB file
func (r *RDBDecoder) peekUInt8() uint8 {
	if r.offset+1 > r.max {
		fmt.Printf("Error: trying to read from pos: %d to %d when the max is %d\n", r.offset, r.offset+1, r.max)
		return 0
	}
	return buffToUInt8(r.data[r.offset : r.offset+1])
}

// Read a string from the RDB file, the length is encoded in the first 2 bits and use readLength to decode it
func (r *RDBDecoder) readString() string {
	length := r.readLength()
	if length == -1 {
		fmt.Println("Unable to read the string, returned length -1")
		return ""
	} else if length == -8 {
		return fmt.Sprintf("%d", r.readUInt8())
	} else if length == -16 {
		return fmt.Sprintf("%d", r.readUInt16())
	} else if length == -32 {
		return fmt.Sprintf("%d", r.readUInt32())
	}

	if r.offset+int(length) > r.max {
		fmt.Printf("Error: trying to read from pos: %d to %d when the max is %d\n", r.offset, r.offset+int(length), r.max)
		return ""
	}
	s := string(r.data[r.offset : r.offset+int(length)])
	r.offset += int(length)
	return s
}

// Read the length of a string, see length encoding in the RDB file:
func (r *RDBDecoder) readLength() int {
	br := NewBitReader(r.data[r.offset:])
	bitLen := br.ReadBits(2)
	switch {
	// The next 6 bits represent the length
	case bitLen == 0:
		len := int(br.ReadBits(6))
		r.offset++
		return len
	// The next 14 bits represent the length
	case bitLen == 1:
		len := int(br.ReadBits(14))
		r.offset += 2
		return len
	// Discard the remaining 6 bites, the next 4 bytes represent the length
	case bitLen == 2:
		r.offset++
		len := int(r.readUInt32())
		return len
	// The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding
	case bitLen == 3:
		// fmt.Println("Special encoding")
		magicBit := int(br.ReadBits(6))
		// fmt.Printf("Magic bit: %d\n", magicBit)
		r.offset++
		switch {
		case magicBit == 0:
			return -8
		case magicBit == 1:
			return -16
		case magicBit == 2:
			return -32
		case magicBit == 3:
			fmt.Println("not implemented")
			return -1
		default:
			return -1
		}
	default:
		r.offset++
		return -1
	}
}

func (r *RDBDecoder) readUInt64() uint64 {
	if r.offset+8 > r.max {
		fmt.Printf("Error: trying to read from pos: %d to %d when the max is %d\n", r.offset, r.offset+8, r.max)
		return 0
	}
	n := buffToUInt64(r.data[r.offset : r.offset+8])
	r.offset += 8
	return n
}

func (r *RDBDecoder) readUInt32() uint32 {
	if r.offset+4 > r.max {
		fmt.Printf("Error: trying to read from pos: %d to %d when the max is %d\n", r.offset, r.offset+4, r.max)
		return 0
	}
	n := buffToUInt32(r.data[r.offset : r.offset+4])
	r.offset += 4
	return n
}

func (r *RDBDecoder) readUInt16() uint16 {
	if r.offset+2 > r.max {
		fmt.Printf("Error: trying to read from pos: %d to %d when the max is %d\n", r.offset, r.offset+2, r.max)
		return 0
	}
	n := buffToUInt16(r.data[r.offset : r.offset+2])
	r.offset += 2
	return n
}

func (r *RDBDecoder) readUInt8() uint8 {
	if r.offset+1 > r.max {
		fmt.Printf("Error: trying to read from pos: %d to %d when the max is %d\n", r.offset, r.offset+1, r.max)
		return 0
	}
	n := buffToUInt8(r.data[r.offset : r.offset+1])
	r.offset += 1
	return n
}
