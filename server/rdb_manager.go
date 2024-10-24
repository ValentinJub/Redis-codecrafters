package server

import (
	"fmt"

	log "github.com/codecrafters-io/redis-starter-go/logger"
	utils "github.com/codecrafters-io/redis-starter-go/utils"
)

const (
	Reset = "\033[0m"
	Pink  = "\033[35m"
)

// Defines the actions to perfom on the RDB file
type RDBManager interface {
	// Returns the directory and file name of the RDB file
	Info() (string, string)
	LoadRDBToCache() error
}

type RDBmanager struct {
	dir    string
	dbfile string
	server *MasterServer
}

func NewRDBManager(dir, dbfile string, s *MasterServer) *RDBmanager {
	return &RDBmanager{dir: dir, dbfile: dbfile, server: s}
}

func (r *RDBmanager) Info() (string, string) {
	return r.dir, r.dbfile
}

// Open the RDB file and load the data into the cache
func (r *RDBmanager) LoadRDBToCache() error {
	buffer, err := utils.ReadFile(r.dir + "/" + r.dbfile)
	if err != nil {
		return err
	}
	data := buffer.Bytes()
	log.LogByteStreamToHex(data)
	objs, err := r.decodeRDB(data)
	if err != nil {
		return err
	}
	// Load the objects into the cache, making sure to not add an object that has expired
	for k, v := range objs {
		if v.expiry != 0 {
			if r.server.cache.IsExpired(k) {
				continue
			}
		}
		err := r.server.cache.Set(k, v.value)
		if err != nil {
			fmt.Printf("error while setting key %s: %s\n", k, err)
		}
	}
	return nil
}

// Decode the RDB data and return a map of keys-values, with an expiry if any
func (r *RDBmanager) decodeRDB(data []byte) (map[string]Object, error) {
	d := NewRDBDecoder(data)
	result, err := d.Decode()
	if err != nil {
		return map[string]Object{}, err
	}
	// Perform type assertion
	objects, ok := result.(map[string]Object)
	if !ok {
		return map[string]Object{}, fmt.Errorf("error: expected map[string]Object")
	}
	return objects, nil
}
