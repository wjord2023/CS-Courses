package shardgrp

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
)

func kvmapToBytes(kvmap kvMap) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kvmap); err != nil {
		panic(fmt.Sprintf("encode kvmap failed: %v", err))
	}
	return w.Bytes()
}

func bytesToKvmap(data []byte) kvMap {
	var kvmap kvMap
	d := bytes.NewBuffer(data)
	e := labgob.NewDecoder(d)
	if err := e.Decode(&kvmap); err != nil {
		panic(fmt.Sprintf("decode kvmap failed: %v", err))
	}
	return kvmap
}
