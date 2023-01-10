package mpTrie

import (
	"github.com/openGemini/openGemini/lib/utils"
	"fmt"
	"hash/crc32"
	"sort"
)

type InvertedItem struct {
	tsid      uint64
	timestamp int64
	posBlock  []uint16
	size      uint64
}

func (i InvertedItem) Tsid() uint64 {
	return i.tsid
}

func (i InvertedItem) Timestamp() int64 {
	return i.timestamp
}

func (i InvertedItem) PosBlock() []uint16 {
	return i.posBlock
}

func (i InvertedItem) Size() uint64 {
	return i.size
}

type InvertedListBlock struct {
	blk     []*InvertedItem
	mpblk   map[utils.SeriesId][]uint16
	blksize uint64
}

func (i *InvertedListBlock) Len() int {
	return len(i.Blk())
}

func (i *InvertedListBlock) Swap(m, n int) {
	blk := i.Blk()
	blk[m], blk[n] = blk[n], blk[m]
}

func (i *InvertedListBlock) Less(m, n int) bool {
	items := i.Blk()
	if items[m].Tsid() != items[n].Tsid() {
		return items[m].Tsid() < items[n].Tsid()
	}
	return items[m].Timestamp() < items[n].Timestamp()
}


func InitInvertedListBlock(mpblk map[utils.SeriesId][]uint16, blksize uint64) *InvertedListBlock {
	return &InvertedListBlock{mpblk: mpblk, blksize: blksize}
}

func (i *InvertedListBlock) Mpblk() map[utils.SeriesId][]uint16 {
	return i.mpblk
}

func (i *InvertedListBlock) SetMpblk(mpblk map[utils.SeriesId][]uint16) {
	i.mpblk = mpblk
}

func (i *InvertedListBlock) SetBlk(blk []*InvertedItem) {
	i.blk = blk
}

func (i *InvertedListBlock) SetBlksize(blksize uint64) {
	i.blksize = blksize
}

func (i InvertedListBlock) Blk() []*InvertedItem {
	return i.blk
}

func (i InvertedListBlock) Blksize() uint64 {
	return i.blksize
}

//invertedlistblock convert hashcode
func (invetdblk *InvertedListBlock) HashInvertedBlk() uint32 {
	sort.Sort(invetdblk)
	items := invetdblk.Blk()
	res := make([]byte, 0)
	for _, one := range items {
		tsidbyte, _ := IntToBytes(int(one.Tsid()), DEFAULT_SIZE)
		timebyte, _ := IntToBytes(int(one.Timestamp()), DEFAULT_SIZE)
		posbyte, _ := IntToBytes(int(one.PosBlock()[0]), 2)
		res = append(res, tsidbyte...)
		res = append(res, timebyte...)
		res = append(res, posbyte...)
	}
	hashcode := crc32.ChecksumIEEE(res)
	return hashcode
}

func PrintInvertedBlk(blk []*InvertedItem) {
	for _, item := range blk {
		fmt.Print("[{", item.Tsid(), item.Timestamp(), "}:", item.PosBlock(), "]")
	}
}

func NewInvertedListBlock(blk []*InvertedItem, size uint64) *InvertedListBlock {
	return &InvertedListBlock{blk: blk, blksize: size}
}

func NewInvertedItem(tsid uint64, timestamp int64, posBlock []uint16, size uint64) *InvertedItem {
	return &InvertedItem{tsid: tsid, timestamp: timestamp, posBlock: posBlock, size: size}
}
