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

func UnserializeInvertedListBlk(offset uint64, file *os.File) *InvertedListBlock {
	sizeByte, seek := ReadOffByteFormFilePtr(file, int64(offset), DEFAULT_SIZE, io.SeekStart)
	blksize, _ := BytesToInt(sizeByte, false)
	data, _ := ReadOffByteFormFilePtr(file, seek, int64(blksize), io.SeekStart)
	mp := decodeMapByGob(data)
	blk := InitInvertedListBlock(mp, uint64(blksize))
	return blk
}

func decodeMapByGob(buffer []byte) map[uint64][]utils.TimePoint {
	res := make(map[uint64][]utils.TimePoint)
	decoder := gob.NewDecoder(bytes.NewBuffer(buffer))
	err := decoder.Decode(&res)
	if err != nil {
		return nil
	}
	//lensum := 0
	//for _, v := range res {
	//	lensum += len(v)
	//}
	//TIMELIMELEN += lensum
	//INVTDMAPLEN += len(res)
	return res
}
