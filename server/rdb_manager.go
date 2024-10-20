package server

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

type RDBManager interface {
	// Read the RDB file and return the keys matching key
	Read(key string) ([]string, error)
	// Returns the directory and file name of the RDB file
	Info() (string, string)
}

type RDB struct {
	dir    string
	dbfile string
}

func NewRDBManager(dir, dbfile string) *RDB {
	return &RDB{dir: dir, dbfile: dbfile}
}

func (r *RDB) Info() (string, string) {
	return r.dir, r.dbfile
}

func (r *RDB) Read(key string) ([]string, error) {
	fmt.Println("Reading from RDB file")
	err := r.openRDBFile()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Create the file - WIP
			return []string{}, err
		}
		return []string{}, err
	}
	return []string{}, nil
}

func (r *RDB) openRDBFile() error {
	f, err := os.Open(r.dir + "/" + r.dbfile)
	if err != nil {
		return err
	}
	defer f.Close()

	b := new(bytes.Buffer)
	_, err = b.ReadFrom(f)
	if err != nil {
		return err
	}

	fmt.Printf("Opened file: %s\n", r.dir+"/"+r.dbfile)
	fmt.Printf("File content (hex): %x\n", b.Bytes())
	return nil
}
