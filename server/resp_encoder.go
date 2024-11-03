package server

import "fmt"

func newSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s%s", s, CRLF))
}

func newBulkString(s string) []byte {
	if s == "" {
		return []byte("$-1" + CRLF)
	}
	return []byte(fmt.Sprintf("$%d%s%s%s", len(s), CRLF, s, CRLF))
}

func newBulkArray(element ...string) []byte {
	str := fmt.Sprintf("*%d%s", len(element), CRLF)
	for _, e := range element {
		str += fmt.Sprintf("$%d%s%s%s", len(e), CRLF, e, CRLF)
	}
	return []byte(str)
}

func newBulkArrayOfArrays(arrays ...string) []byte {
	str := []byte(fmt.Sprintf("*%d%s", len(arrays), CRLF))
	for _, array := range arrays {
		str = append(str, []byte(array)...)
	}
	return str
}

func newInteger(i int) []byte {
	return []byte(fmt.Sprintf(":%d%s", i, CRLF))
}

func newSimpleError(s string) []byte {
	return []byte(fmt.Sprintf("-%s%s", s, CRLF))
}
