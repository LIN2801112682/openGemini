package mpTrie

import (
	"bytes"
	"encoding/gob"
	"github.com/openGemini/openGemini/lib/utils"
	"io"
	"os"
)

/**
* @ Author: Yaixihn
* @ Dec:map+trie unserialized
* @ Date: 2022/10/5 13:40
 */


func UnserializeInvertedListBlk(offset uint64,file *os.File) *InvertedListBlock {
	sizeByte,seek := ReadOffByteFormFilePtr(file,int64(offset), DEFAULT_SIZE,io.SeekStart)
	blksize, _ := BytesToInt(sizeByte, false)
	data, _ := ReadOffByteFormFilePtr(file, seek, int64(blksize), io.SeekStart)
	mp := decodeMapByGob(data)
	blk := InitInvertedListBlock(mp, uint64(blksize))
	return blk
}

func decodeMapByGob(buffer []byte) map[utils.SeriesId][]uint16 {
	res := make(map[utils.SeriesId][]uint16)
	decoder := gob.NewDecoder(bytes.NewBuffer(buffer))
	err := decoder.Decode(&res)
	if err != nil {
		return nil
	}
	return res
}
