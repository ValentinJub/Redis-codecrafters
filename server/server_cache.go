package server

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Cache interface {
	Set(key string, value string) error
	SetExpiry(key string, value string, expiry uint64) error
	SetStream(key, id string, fields map[string]string) error
	Get(key string) (string, error)
	Keys(key string) []string
	Type(key string) string
	ExpireIn(key string, milliseconds uint64) error
	IsExpired(key string) bool
}

type CacheImpl struct {
	cache map[string]Object
}

type Object struct {
	value  string
	expiry uint64
	stream *Stream
}

/*

entries:
  - id: 1526985054069-0 # (ID of the first entry)
    temperature: 36 # (A key value pair in the first entry)
    humidity: 95 # (Another key value pair in the first entry)

  - id: 1526985054079-0 # (ID of the second entry)
    temperature: 37 # (A key value pair in the first entry)
    humidity: 94 # (Another key value pair in the first entry)

  # ... (and so on)

*/

type Stream struct {
	entries []StreamEntry
}

type StreamEntry struct {
	id     string
	fields map[string]string
}

func NewCache() *CacheImpl {
	return &CacheImpl{cache: make(map[string]Object)}
}

func newStream() *Stream {
	return &Stream{entries: make([]StreamEntry, 0)}
}

func (s *CacheImpl) Set(key string, value string) error {
	s.cache[key] = Object{value: value, stream: nil}
	return nil
}

// Create or append to a stream
func (s *CacheImpl) SetStream(key, id string, fields map[string]string) error {
	if v, ok := s.cache[key]; ok {
		if v.stream == nil {
			v.stream = newStream()
		}
		v.stream.entries = append(v.stream.entries, StreamEntry{id: id, fields: fields})
		s.cache[key] = v
	} else {
		s.cache[key] = Object{stream: &Stream{entries: []StreamEntry{{id: id, fields: fields}}}}
	}
	return nil
}

func (s *CacheImpl) SetExpiry(key string, value string, expiry uint64) error {
	s.cache[key] = Object{value: value, expiry: expiry}
	return nil
}

// Need to edit this to return the object instead of the value
func (s *CacheImpl) Get(key string) (string, error) {
	if v, ok := s.cache[key]; ok {
		if s.IsExpired(key) {
			return "", fmt.Errorf("key expired")
		}
		return v.value, nil
	} else {
		return "", fmt.Errorf("key not found")
	}
}

// Return the keys matching the pattern
func (s *CacheImpl) Keys(key string) []string {
	keys := make([]string, 0)
	keyRegexp := parseKey(key)
	fmt.Printf("keyRegexp: %s\n", keyRegexp.String())
	for k := range s.cache {
		if keyRegexp.MatchString(k) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Return the type of the key
func (s *CacheImpl) Type(key string) string {
	if v, ok := s.cache[key]; ok {
		if v.stream != nil {
			return "stream"
		}
		return "string"
	}
	return "none"
}

/*
Return a regex pattern made of key
Supported glob-style patterns:

	h?llo matches hello, hallo and hxllo
	h*llo matches hllo and heeeello
	h[ae]llo matches hello and hallo, but not hillo
	h[^e]llo matches hallo, hbllo, ... but not hello
	h[a-b]llo matches hallo and hbllo

Not implemented yet: Use \ to escape special characters if you want to match them verbatim.
*/
func parseKey(key string) *regexp.Regexp {
	key = strings.ReplaceAll(key, "*", ".*")
	key = strings.ReplaceAll(key, "?", ".")
	key = strings.ReplaceAll(key, "[", "[^")
	key = strings.ReplaceAll(key, "]", "]")
	return regexp.MustCompile(key)
}

// ExpireIn sets the expiry time of a key in milliseconds from now
func (s *CacheImpl) ExpireIn(key string, milliseconds uint64) error {
	if v, ok := s.cache[key]; !ok {
		return fmt.Errorf("key not found")
	} else {
		now := time.Now().UnixMilli()
		v.expiry = uint64(now) + milliseconds
		s.cache[key] = v
		return nil
	}
}

func (s *CacheImpl) IsExpired(key string) bool {
	if v, ok := s.cache[key]; ok {
		if v.expiry != 0 {
			now := time.Now().UnixMilli()
			if uint64(now) >= v.expiry {
				delete(s.cache, key)
				return true
			}
		}
	} else {
		return true
	}
	return false
}
