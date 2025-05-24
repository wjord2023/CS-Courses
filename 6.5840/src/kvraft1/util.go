package kvraft

import (
	"crypto/rand"
	"encoding/hex"
	"log"
)

const debug = false

func DPrintf(device string, format string, a ...any) {
	if debug {
		switch device {
		case "server":
			format = "\033[31m[server]\033[0m  " + format
		case "client":
			format = "\033[32m[client]\033[0m  " + format
		}

		log.Printf(format, a...)
	}
}

func RandID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
