package mpTrie

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"
)

/**
* @ Author: Yaixihn
* @ Dec:map+trie unserialized
* @ Date: 2022/10/4 13:40
 */

//todo
func UnserializeAddrListBlk(offset uint64, file *os.File) *AddrListBlock {
	sizebyte,seek := ReadOffByteFormFilePtr(file,int64(offset), DEFAULT_SIZE,io.SeekStart)
	blksize, _ := BytesToInt(sizebyte, false)
	buffer, _ := ReadOffByteFormFilePtr(file, seek, int64(blksize), io.SeekStart)
	mp := UnserializeAddrMap(buffer)
	blk := InitAddrListBlock(mp, uint64(blksize))
	return blk
}

func UnserializeAddrMap(buffer []byte) map[uint64]uint16 {
	res := make(map[uint64]uint16)
	decoder := gob.NewDecoder(bytes.NewBuffer(buffer))
	err := decoder.Decode(&res)
	if err != nil {
		return nil
	}
	return res
}
