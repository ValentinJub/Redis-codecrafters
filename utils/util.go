package utils

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
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

// The ID can be any pseudo random alphanumeric string of 40 characters.
func CreateReplicationID() string {
	// Create a function that generates a random ID made of alphanumeric characters of length 40
	id, err := generateRandomString(40)
	if err != nil {
		return ""
	}
	return id
}

// GenerateRandomString generates a random alphanumeric string of specified length
func generateRandomString(n int) (string, error) {
	// Generate random bytes, enough to be encoded as a base64 string of at least length n
	byteLen := (n*6 + 7) / 8
	bytes := make([]byte, byteLen)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	// Encode bytes to base64 and strip any non-alphanumeric characters if necessary
	str := base64.URLEncoding.EncodeToString(bytes)
	str = strings.ReplaceAll(str, "-", "1")
	str = strings.ReplaceAll(str, "_", "2")
	return str[:n], nil
}

func ParseOsArgs(args []string) (map[string]string, error) {
	argsMap := make(map[string]string)
	for x, arg := range args {
		switch arg {
		case "--dir":
			if x+1 < len(args) {
				argsMap[arg] = args[x+1]
				fmt.Printf("dir: %s\n", args[x+1])
			} else {
				return nil, fmt.Errorf("missing argument for --dir")
			}
		case "--dbfilename":
			if x+1 < len(args) {
				argsMap[arg] = args[x+1]
				fmt.Printf("dbfilename: %s\n", args[x+1])
			} else {
				return nil, fmt.Errorf("missing argument for --dbfilename")
			}
		case "--port":
			if x+1 < len(args) {
				argsMap[arg] = args[x+1]
				fmt.Printf("port: %s\n", args[x+1])
			} else {
				return nil, fmt.Errorf("missing argument for --port")
			}
		case "--replicaof":
			if x+1 < len(args) {
				parts := strings.Split(args[x+1], " ")
				if parts[0] == "localhost" {
					parts[0] = "127.0.0.1"
				}
				argsMap[arg] = strings.Join(parts, ":")
				fmt.Printf("replicaof: %s\n", argsMap[arg])
			} else {
				return nil, fmt.Errorf("missing argument for --replicaof")
			}
		default:
		}
	}
	return argsMap, nil
}
