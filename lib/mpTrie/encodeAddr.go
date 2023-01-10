package mpTrie

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
)


func encodeTokenAddrCntStatus(addoff map[*tokenIndex.IndexTreeNode]uint16) []*AddrCenterStatus {
	res := make([]*AddrCenterStatus, 0)
	for node, off := range addoff {
		cur := *node
		inverted := cur.InvertedIndex()
		mpblk := encodeInvertedBlk(inverted)
		tmp := NewAddrCenterStatus(mpblk, off)
		res = append(res, tmp)
	}
	return res
}

func encodeGramAddrCntStatus(addoff map[*gramIndex.IndexTreeNode]uint16) []*AddrCenterStatus {
	res := make([]*AddrCenterStatus, 0)
	for node, off := range addoff {
		cur := *node
		inverted := cur.InvertedIndex()
		mpblk := encodeInvertedBlk(inverted)
		tmp := NewAddrCenterStatus(mpblk, off)
		res = append(res, tmp)
	}
	return res
}

//mapenc version update addrlistblock offset
func addrCenterStatusToBLK(startoff uint64, idxData []string, res_addrctr map[string][]*AddrCenterStatus, invtdblkHashToOff map[uint32]uint64) (map[string]*AddrListBlock, map[*AddrListBlock]uint64) {
	mp_addrblk := make(map[string]*AddrListBlock)
	addrblkToOff := make(map[*AddrListBlock]uint64, 0) //<addrlistblock,offset> of the addrlistblock
	for _, data := range idxData {
		addrblk := new(AddrListBlock)
		ctrstatusArry := res_addrctr[data]
		items := make(map[uint64]uint16)
		for _, ctrstatus := range ctrstatusArry {
			blk := ctrstatus.Blk()
			logoff := ctrstatus.Offset()
			//diff addr
			hash := HashInvertedBlk(blk)
			invtdoffset := invtdblkHashToOff[hash]
			items[invtdoffset]=logoff
		}
		addrbytes,err := EncodeAddrByGob(items)
		blksize := uint64(len(addrbytes))
		if err!=nil{
			fmt.Println(err)
			return nil,nil
		}
		addrblk.SetMpblk(items)
		addrblk.SetBlksize(blksize)
		mp_addrblk[data] = addrblk
		addrblkToOff[addrblk] = startoff
		startoff += addrblk.Blksize() + DEFAULT_SIZE
	}
	return mp_addrblk, addrblkToOff
}

func EncodeAddrByGob(mp map[uint64]uint16) ([]byte,error){
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(mp)
	if err != nil {
		return nil,fmt.Errorf("encodeaddr failed.")
	}
	return buf.Bytes(),nil
}

//serialize addrlistblock
func serializeAddrListBlock(addrblk *AddrListBlock) []byte {
	res := make([]byte, 0)
	addrblksize, err := IntToBytes(int(addrblk.Blksize()), stdlen)
	res = append(res, addrblksize...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	blk := addrblk.Mpblk()
	byGob, err := EncodeAddrByGob(blk)
	if err != nil {
		return nil
	}
	res = append(res,byGob...)
	return res
}

