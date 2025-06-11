package shardutils

import "log"

var Debug bool = true // set to false to disable debug output

func DPrintf(dtype string, format string, a ...any) {
	if Debug {
		switch dtype {
		case "shardkv":
			format = "\033[31m[shardkv]\033[0m  " + format
		case "shardcfg":
			format = "\033[32m[shardcfg]\033[0m  " + format
		case "shardgrp":
			format = "\033[33m[shardgrp]\033[0m  " + format
		case "shardctrler":
			format = "\033[34m[shardctrler]\033[0m  " + format
		}
		log.Printf(format, a...)
	}
}
