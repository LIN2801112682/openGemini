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


func EncodemapByGob(inverted  map[uint64][]utils.TimePoint) ([]byte,error){
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(inverted)
	if err != nil {
		return nil,fmt.Errorf("encodeInvertedMap ERROR.")
	}
	//check := decodeMapByGob(buf.Bytes())
	//if len(inverted)!=len(check){
	//	log.Println("encode and decode is different.")
	//}
	return buf.Bytes(),nil
}

//turn to file layout
func encodeInvertedBlk(inverted  map[uint64][]utils.TimePoint) *InvertedListBlock {
	buf,err := EncodemapByGob(inverted)
	if err!=nil{
		fmt.Println(err)
		return nil
	}
	size := len(buf)
	blk :=  InitInvertedListBlock(inverted, uint64(size))
	return blk
}


func GetMaxAndMinTime(index  map[uint64][]utils.TimePoint) (min, max int64) {
	sortTime := make([]int64, 0)
	if len(index) <= 0 {
		return -1, -1
	}
	for _, value := range index {
		for _,timepoint := range value{
			sortTime = append(sortTime, timepoint.TimeStamp)
		}
	}
	sort.Slice(sortTime, func(i, j int) bool {
		return sortTime[i]< sortTime[j]
	})
	return sortTime[0], sortTime[len(sortTime)-1]
}
