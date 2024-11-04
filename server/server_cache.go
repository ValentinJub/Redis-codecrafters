package server

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Cache interface {
	Set(key string, value string) error
	SetExpiry(key string, value string, expiry uint64) error
	SetStream(key, id string, fields map[string]string) (string, error)
	Get(key string) (string, error)
	GetStream(key string, start, end int) ([]StreamEntry, error)
	GetLastEntryFromStream(key string) (StreamEntry, error)
	Increment(key string) (int, error)
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

func (s *StreamEntry) Values() (string, []string) {
	values := make([]string, 0)
	for k, v := range s.fields {
		values = append(values, k, v)
	}
	return s.id, values
}

func (s *StreamEntry) ID() string {
	return s.id
}

func (s *StreamEntry) IsEmpty() bool {
	return len(s.fields) == 0 && s.id == ""
}

func NewCache() *CacheImpl {
	return &CacheImpl{cache: make(map[string]Object)}
}

func (s *CacheImpl) Set(key string, value string) error {
	s.cache[key] = Object{value: value, stream: nil}
	return nil
}

func (s *CacheImpl) Increment(key string) (int, error) {
	if v, ok := s.cache[key]; ok {
		i, err := strconv.Atoi(v.value)
		if err != nil {
			return 0, err
		}
		i++
		s.cache[key] = Object{value: strconv.Itoa(i), stream: nil}
		return i, nil
	} else {
		s.cache[key] = Object{value: "1", stream: nil}
		return 1, nil
	}
}

func (s *CacheImpl) GetStream(key string, start, end int) ([]StreamEntry, error) {
	if v, ok := s.cache[key]; ok {
		if v.stream == nil {
			return nil, fmt.Errorf("ERR The key is not a stream")
		}
		entries := make([]StreamEntry, 0)
		for _, entry := range v.stream.entries {
			entryID, err := strconv.Atoi(strings.ReplaceAll(entry.id, "-", ""))
			if err != nil {
				return nil, err
			}
			if entryID >= start && entryID <= end {
				entries = append(entries, entry)
			}
		}
		return entries, nil
	}
	return nil, fmt.Errorf("ERR The key does not exist")
}

func (s *CacheImpl) GetLastEntryFromStream(key string) (StreamEntry, error) {
	if v, ok := s.cache[key]; ok {
		if v.stream == nil {
			return StreamEntry{}, fmt.Errorf("ERR The key is not a stream")
		}
		if len(v.stream.entries) == 0 {
			return StreamEntry{}, nil
		}
		return v.stream.entries[len(v.stream.entries)-1], nil
	}
	return StreamEntry{}, fmt.Errorf("ERR The key does not exist")
}

// Create or append to a stream
func (s *CacheImpl) SetStream(key, id string, fields map[string]string) (string, error) {
	if !isValidID(id) {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	type_, ms, seq, err := decodeID(id)
	if err != nil {
		return "", err
	}
	v, keyExists := s.cache[key]
	switch type_ {
	case AUTO:
		ms = int(time.Now().UnixMilli())
		if keyExists {
			seq, err = defineSeq(v.stream, ms)
			if err != nil {
				return "", err
			}
		} else {
			seq = 0
		}
		id = fmt.Sprintf("%d-%d", ms, seq)
		if keyExists {
			v.stream.entries = append(v.stream.entries, StreamEntry{id: id, fields: fields})
			s.cache[key] = v
		} else {
			s.cache[key] = Object{stream: &Stream{entries: []StreamEntry{{id: id, fields: fields}}}}
		}
	case INCREMENT:
		if keyExists {
			// Define the sequence number, incrementing from the last ID if the last ID equals the current ID
			seq, err = defineSeq(v.stream, ms)
			if err != nil {
				return "", err
			}
		} else {
			if ms == 0 {
				seq = 1
			} else {
				seq = 0
			}
		}
		id = fmt.Sprintf("%d-%d", ms, seq)
		if keyExists {
			v.stream.entries = append(v.stream.entries, StreamEntry{id: id, fields: fields})
			s.cache[key] = v
		} else {
			s.cache[key] = Object{stream: &Stream{entries: []StreamEntry{{id: id, fields: fields}}}}
		}
	case CLIENT:
		if keyExists {
			if len(v.stream.entries) != 0 {
				lastID := v.stream.entries[len(v.stream.entries)-1].id
				_, lastMs, lastSeq, _ := decodeID(lastID)
				if lastMs == ms {
					if lastSeq >= seq {
						return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
					}
				} else if lastMs > ms {
					return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
				}
			}
		} else {
			if (ms == 0 && seq < 1) || ms < 0 || (ms > 0 && seq < 0) {
				return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			}
		}
		id = fmt.Sprintf("%d-%d", ms, seq)
		if keyExists {
			v.stream.entries = append(v.stream.entries, StreamEntry{id: id, fields: fields})
			s.cache[key] = v
		} else {
			s.cache[key] = Object{stream: &Stream{entries: []StreamEntry{{id: id, fields: fields}}}}
		}
	default:
		return "", fmt.Errorf("ERR Invalid ID type")
	}
	fmt.Printf("About to return id: %s\n", id)
	return id, nil
}

// Define the next sequence number depending on the last ID
func defineSeq(stream *Stream, ms int) (int, error) {
	seq := 0
	if len(stream.entries) == 0 {
		seq = 1
	} else {
		lastID := stream.entries[len(stream.entries)-1].id
		_, lastMs, lastSeq, _ := decodeID(lastID)
		if lastMs == ms {
			seq = lastSeq + 1
		} else if lastMs < ms {
			seq = 0
		} else {
			return -1, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}
	return seq, nil
}

func (s *CacheImpl) SetExpiry(key string, value string, expiry uint64) error {
	s.cache[key] = Object{value: value, expiry: expiry}
	return nil
}

const (
	AUTO = iota
	INCREMENT
	CLIENT
)

// Type auto 0 - incrementing 1 - provided by the client 2
func decodeID(id string) (type_, ms, seq int, err error) {
	// Auto generatedID
	if id == "*" {
		return
	}
	parts := strings.Split(id, "-")
	ms, _ = strconv.Atoi(parts[0])
	if parts[1] == "*" {
		return INCREMENT, ms, 0, nil
	}
	seq, _ = strconv.Atoi(parts[1])
	return CLIENT, ms, seq, nil
}

func isValidID(id string) bool {
	if id == "*" {
		return true
	}
	parts := strings.Split(id, "-")
	ms, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}
	if parts[1] == "*" {
		return ms >= 0
	}
	seq, err := strconv.Atoi(parts[1])
	if err != nil {
		return false
	}
	if ms < 0 || seq < 1 {
		return false
	}
	return true
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
