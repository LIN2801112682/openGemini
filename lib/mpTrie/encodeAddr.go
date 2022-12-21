package mpTrie

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
	"reflect"
	"sort"
)

//the center status of addrlistblock turn to file layout format
func encodeTokenAddrCntStatus(addoff map[*tokenIndex.IndexTreeNode]uint16) []*AddrCenterStatus {
	res := make([]*AddrCenterStatus, 0)
	for node, off := range addoff {
		cur := *node
		inverted := cur.InvertedIndex()
		blk := encodeInvertedBlk(inverted)
		tmp := NewAddrCenterStatus(blk, off)
		res = append(res, tmp)
	}
	return res
}
func encodeGramAddrCntStatus(addoff map[*gramIndex.IndexTreeNode]uint16) []*AddrCenterStatus {
	res := make([]*AddrCenterStatus, 0)
	for node, off := range addoff {
		cur := *node
		inverted := cur.InvertedIndex()
		blk := encodeInvertedBlk(inverted)
		tmp := NewAddrCenterStatus(blk, off)
		res = append(res, tmp)
	}
	return res
}

//update addrlistblock offset
func addrCenterStatusToBLK(startoff uint64, idxData []string, res_addrctr map[string][]*AddrCenterStatus, invtdblkHashToOff map[uint32]uint64) (map[string]*AddrListBlock, map[*AddrListBlock]uint64) {
	mp_addrblk := make(map[string]*AddrListBlock)
	addrblkToOff := make(map[*AddrListBlock]uint64, 0) //&lt;addrlistblock,offset&gt; of the addrlistblock
	for _, data := range idxData {
		addrblk := new(AddrListBlock)
		ctrstatusArry := res_addrctr[data]
		items := make([]*AddrItem, 0)
		var blksize uint64 = 0
		for _, ctrstatus := range ctrstatusArry {
			blk := ctrstatus.Blk()
			logoff := ctrstatus.Offset()
			//diff addr
			hash := blk.HashInvertedBlk()
			invtdoffset := invtdblkHashToOff[hash]

			item := NewAddrItem(invtdoffset, logoff)
			items = append(items, item)
			blksize += item.Size() + DEFAULT_SIZE
		}
		addrblk.SetBlk(items)
		addrblk.SetBlksize(blksize)
		mp_addrblk[data] = addrblk
		addrblkToOff[addrblk] = startoff
		startoff += addrblk.Blksize() + DEFAULT_SIZE
	}
	return mp_addrblk, addrblkToOff
}

func checkblk(blk *InvertedListBlock, invtdblkToOff map[*InvertedListBlock]uint64) (uint64, error) {
	for tmp, off := range invtdblkToOff {
		sort.Sort(tmp)
		sort.Sort(blk)
		if reflect.DeepEqual(tmp, blk) {
			return off, nil
		}
	}
	return 3, fmt.Errorf("AddrcenterStatus not find Inverted offset.")
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
	blk := addrblk.Blk()
	for _, item := range blk {
		b := serializeAddrItem(item)
		res = append(res, b...)
	}
	return res
}

func serializeAddrItem(item *AddrItem) []byte {
	res := make([]byte, 0)
	itemsize, err := IntToBytes(int(item.Size()), stdlen)
	res = append(res, itemsize...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	itemdata, err := IntToBytes(int(item.Addrdata()), stdlen)
	res = append(res, itemdata...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	itemoff, err := IntToBytes(int(item.IndexEntryOffset()), 2)
	res = append(res, itemoff...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return res
}
