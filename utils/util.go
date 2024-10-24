package utils

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

func ReadFile(file string) (*bytes.Buffer, error) {
	fileHandle, err := os.Open(file)
	if err != nil {
		return new(bytes.Buffer), fmt.Errorf("unable to open %s\nError: %s", file, err)
	}
	defer fileHandle.Close()
	// Put the file data in a buffer we can read from
	b := new(bytes.Buffer)
	_, err = io.Copy(b, fileHandle)
	if err != nil {
		return new(bytes.Buffer), fmt.Errorf("error while reading from the file: %s", err)
	}
	return b, nil
}
