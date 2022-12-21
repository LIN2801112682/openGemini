package mpTrie

import (
	"github.com/openGemini/openGemini/lib/utils"
)

func UnserializeInvertedListBlk(offset uint64, buffer []byte) *InvertedListBlock {
	buf := buffer[offset:]
	sizeByte := buf[:DEFAULT_SIZE]
	blksize, _ := BytesToInt(sizeByte, false)
	data := buf[DEFAULT_SIZE:]
	data = data[:blksize]
	//items := unserializeInvertedItem(data)
	mp := UnserializeInvertedMap(data)
	blk := InitInvertedListBlock(mp, uint64(blksize))
	return blk
}

func unserializeInvertedItem(buffer []byte) []*InvertedItem {
	list := make([]*InvertedItem, 0)
	for len(buffer) != 0 {
		sizeByte := buffer[:DEFAULT_SIZE]
		itemsize, _ := BytesToInt(sizeByte, false)
		buffer = buffer[DEFAULT_SIZE:]
		data := buffer[:itemsize]
		//tsid
		tsidbyte := data[:DEFAULT_SIZE]
		tsid, _ := BytesToInt(tsidbyte, false)
		//timestamp
		timebyte := data[DEFAULT_SIZE : 2*DEFAULT_SIZE]
		time, _ := BytesToInt(timebyte, false)
		data = data[2*DEFAULT_SIZE:]
		//pos
		posbuf := make([]uint16, 0)
		for len(data) != 0 {
			tmpbyte := data[:2]
			pos, _ := BytesToInt(tmpbyte, false)
			data = data[2:]
			posbuf = append(posbuf, uint16(pos))
		}
		item := NewInvertedItem(uint64(tsid), int64(time), posbuf, uint64(itemsize))
		list = append(list, item)
		buffer = buffer[itemsize:]
	}
	return list
}
func UnserializeInvertedMap(buffer []byte) map[utils.SeriesId][]uint16 {
	res := make(map[utils.SeriesId][]uint16)
	for len(buffer) != 0 {
		sizeByte := buffer[:DEFAULT_SIZE]
		itemsize, _ := BytesToInt(sizeByte, false)
		buffer = buffer[DEFAULT_SIZE:]
		data := buffer[:itemsize]
		//tsid
		tsidbyte := data[:DEFAULT_SIZE]
		tsid, _ := BytesToInt(tsidbyte, false)
		//timestamp
		timebyte := data[DEFAULT_SIZE : 2*DEFAULT_SIZE]
		time, _ := BytesToInt(timebyte, false)
		data = data[2*DEFAULT_SIZE:]
		//pos
		posbuf := make([]uint16, 0)
		for len(data) != 0 {
			tmpbyte := data[:2]
			pos, _ := BytesToInt(tmpbyte, false)
			data = data[2:]
			posbuf = append(posbuf, uint16(pos))
		}
		sid := utils.NewSeriesId(uint64(tsid), int64(time))
		res[sid] = posbuf
		buffer = buffer[itemsize:]
	}
	return res
}
