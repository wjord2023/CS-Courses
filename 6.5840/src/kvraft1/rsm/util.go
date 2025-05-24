package rsm

import "log"

const debug = false

func DPrintf(format string, a ...any) {
	if debug {
		log.Printf(format, a...)
	}
}
