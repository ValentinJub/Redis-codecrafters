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
	Get(key string) (string, error)
	Keys(key string) []string
	ExpireIn(key string, milliseconds uint64) error
	IsExpired(key string) bool
}

type ServerCache struct {
	cache map[string]Object
}

type Object struct {
	value  string
	expiry uint64
}

func NewServerCache() *ServerCache {
	return &ServerCache{cache: make(map[string]Object)}
}

func (s *ServerCache) Set(key string, value string) error {
	s.cache[key] = Object{value: value}
	return nil
}

func (s *ServerCache) SetExpiry(key string, value string, expiry uint64) error {
	s.cache[key] = Object{value: value, expiry: expiry}
	return nil
}

func (s *ServerCache) Get(key string) (string, error) {
	if v, ok := s.cache[key]; ok {
		if s.IsExpired(key) {
			return "", fmt.Errorf("key expired")
		}
		return v.value, nil
	} else {
		return "", fmt.Errorf("key not found")
	}
}

func (s *ServerCache) Keys(key string) []string {
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
func (s *ServerCache) ExpireIn(key string, milliseconds uint64) error {
	if v, ok := s.cache[key]; !ok {
		return fmt.Errorf("key not found")
	} else {
		now := time.Now().UnixMilli()
		v.expiry = uint64(now) + milliseconds
		s.cache[key] = v
		return nil
	}
}

func (s *ServerCache) IsExpired(key string) bool {
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
