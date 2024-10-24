package logger

import (
	"encoding/hex"
	"fmt"
)

const (
	Reset = "\033[0m"
	Pink  = "\033[35m"
)

func LogByteStreamToHex(data []byte) {
	// Log data as hex and ASCII
	x := hex.EncodeToString(data)
	xclean := ""
	for i, char := range x {
		if i%32 == 0 && i != 0 {
			xclean += "\n"
		} else if i%2 == 0 && i != 0 {
			xclean += " "
		}
		xclean += string(char)
	}
	fmt.Printf("%sByte to (hex):\n%s%s\n", Pink, xclean, Reset)
}
