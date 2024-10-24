package server

import "fmt"

type RDBHeader struct {
	magic        string
	dbversionnum int
}

type RDBMetadata struct {
	data map[string]string
}

type RDBdatabase struct {
	dbIndex      int
	numOfKeys    int
	numOfExpires int
	objects      map[string]Object
}

func NewRDBDatabase() *RDBdatabase {
	return &RDBdatabase{objects: make(map[string]Object)}
}

func (r RDBHeader) String() string {
	return fmt.Sprintf("Header:\nMagic: %s, DBVersion: %d\n", r.magic, r.dbversionnum)
}

func (r RDBMetadata) String() string {
	str := "Metadata:\n"
	for k, v := range r.data {
		str += fmt.Sprintf("%s: %s\n", k, v)
	}
	return str
}

func (r RDBdatabase) String() string {
	str := fmt.Sprintf("Database:\nDBIndex: %d\nNumOfKeys: %d\nNumOfExpires: %d\n", r.dbIndex, r.numOfKeys, r.numOfExpires)
	for k, v := range r.objects {
		str += fmt.Sprintf("Key: %s, Value: %s, Expiry: %d\n", k, v.value, v.expiry)
	}
	return str
}
