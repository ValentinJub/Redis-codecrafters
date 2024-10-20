package server

import (
	"fmt"
	"time"
)

type Cache interface {
	Set(key string, value string) error
	Get(key string) (string, error)
	Expire(key string, milliseconds int64) error
}

type ServerCache struct {
	cache map[string]Object
}

type Object struct {
	value  string
	expiry int64
}

func NewServerCache() *ServerCache {
	return &ServerCache{cache: make(map[string]Object)}
}

func (s *ServerCache) Set(key string, value string) error {
	s.cache[key] = Object{value: value}
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

func (s *ServerCache) Expire(key string, milliseconds int64) error {
	if v, ok := s.cache[key]; !ok {
		return fmt.Errorf("key not found")
	} else {
		now := time.Now().UnixMilli()
		v.expiry = now + milliseconds
		s.cache[key] = v
		return nil
	}
}

func (s *ServerCache) IsExpired(key string) bool {
	if v, ok := s.cache[key]; ok {
		if v.expiry != 0 {
			now := time.Now().UnixMilli()
			if now > v.expiry {
				delete(s.cache, key)
				return true
			}
			return false
		}
		return false
	} else {
		return true
	}
}
