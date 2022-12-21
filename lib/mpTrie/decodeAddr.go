package mpTrie

func UnserializeAddrListBlk(offset uint64, buffer []byte) *AddrListBlock {
	buffer = buffer[offset:]
	sizebyte := buffer[:DEFAULT_SIZE]
	blksize, _ := BytesToInt(sizebyte, false)
	buffer = buffer[DEFAULT_SIZE:]
	buffer = buffer[:blksize]
	//items := unserializeAddrItemList(buffer)
	mp := UnserializeAddrMap(buffer)
	blk := InitAddrListBlock(mp, uint64(blksize))
	return blk
}

func unserializeAddrItemList(buffer []byte) []*AddrItem {
	list := make([]*AddrItem, 0)
	for len(buffer) != 0 {
		//size
		sizebyte := buffer[:DEFAULT_SIZE]
		itemsize, _ := BytesToInt(sizebyte, false)
		buffer = buffer[DEFAULT_SIZE:]
		item := unserializeAddrItem(buffer)
		list = append(list, item)
		buffer = buffer[itemsize:]
	}
	return list
}

func unserializeAddrItem(buffer []byte) *AddrItem {
	//data
	databyte := buffer[:DEFAULT_SIZE]
	data, _ := BytesToInt(databyte, false)
	buffer = buffer[DEFAULT_SIZE:]
	//offset
	offbyte := buffer[:2]
	off, _ := BytesToInt(offbyte, false)
	buffer = buffer[2:]
	item := NewAddrItem(uint64(data), uint16(off))
	return item
}

func UnserializeAddrMap(buffer []byte) map[uint64]uint16 {
	res := make(map[uint64]uint16)
	for len(buffer) != 0 {
		//size
		sizebyte := buffer[:DEFAULT_SIZE]
		itemsize, _ := BytesToInt(sizebyte, false)
		buffer = buffer[DEFAULT_SIZE:]
		data, offset := getUnserializeAddrData(buffer)
		res[data] = offset
		buffer = buffer[itemsize:]
	}
	return res
}

func getUnserializeAddrData(buffer []byte) (uint64, uint16) {
	//data
	databyte := buffer[:DEFAULT_SIZE]
	data, _ := BytesToInt(databyte, false)
	buffer = buffer[DEFAULT_SIZE:]
	//offset
	offbyte := buffer[:2]
	off, _ := BytesToInt(offbyte, false)
	buffer = buffer[2:]
	return uint64(data), uint16(off)
}
