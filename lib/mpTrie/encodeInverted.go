package mpTrie

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
	"sort"
)

//serialize to file in bytes array formats
func serializeInvertedListBlk(invtdblk *InvertedListBlock) []byte {
	res := make([]byte, 0)
	// size  store
	invtdblksize, err := IntToBytes(int(invtdblk.Blksize()), stdlen)
	res = append(res, invtdblksize...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	//block
	blk := invtdblk.Mpblk()
	mpbyte, err := EncodemapByGob(blk)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	res = append(res,mpbyte...)
	return res
}


func EncodemapByGob(inverted map[utils.SeriesId][]uint16) ([]byte,error){
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(inverted)
	if err != nil {
		return nil,fmt.Errorf("encodeInvertedMap ERROR.")
	}
	return buf.Bytes(),nil
}

//turn to file layout
func encodeInvertedBlk(inverted map[utils.SeriesId][]uint16) *InvertedListBlock {
	buf,err := EncodemapByGob(inverted)
	if err!=nil{
		fmt.Println(err)
		return nil
	}
	size := len(buf)
	blk :=  InitInvertedListBlock(inverted, uint64(size))
	return blk
}


func GetMaxAndMinTime(index map[utils.SeriesId][]uint16) (min, max int64) {
	sidArr := make([]utils.SeriesId, 0)
	if len(index) <= 0 {
		return -1, -1
	}
	for series, _ := range index {
		sidArr = append(sidArr, series)
	}
	sort.Slice(sidArr, func(i, j int) bool {
		if sidArr[i].Time < sidArr[j].Time {
			return true
		} else {
			return false
		}
	})
	return sidArr[0].Time, sidArr[len(sidArr)-1].Time
}
