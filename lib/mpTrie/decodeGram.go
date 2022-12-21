package mpTrie

import (
	"fmt"
	"os"
)

func UnserializeGramIndexFromFile(buffer []byte, filesize int64, addrCachesize, invtdCachesize int) (*SearchTree, *AddrCache, *InvertedCache) {
	raw := buffer
	if buffer == nil || filesize == 0 || filesize < 2*DEFAULT_SIZE {
		return nil, nil, nil
	}
	invtdTotalbyte := buffer[filesize-2*DEFAULT_SIZE : filesize-DEFAULT_SIZE]
	addrTotalbyte := buffer[filesize-DEFAULT_SIZE:]
	invtdTotal, _ := BytesToInt(invtdTotalbyte, true)
	addrTotal, _ := BytesToInt(addrTotalbyte, true)
	clvdataStart := invtdTotal + addrTotal
	clvdatabuf := buffer[clvdataStart : filesize-2*DEFAULT_SIZE]

	//decode obj
	tree, addrcache, invtdcache := unserializeObj(clvdatabuf, raw, addrCachesize, invtdCachesize)
	return tree, addrcache, invtdcache
}
func GetBytesFromFile(filename string) ([]byte, int64) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}
	filesize := fileinfo.Size()
	buffer := make([]byte, filesize)
	_, err = file.Read(buffer)
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}
	return buffer, filesize
}

func unserializeObj(buffer, raw []byte, addrCachesize, invtdCachesize int) (*SearchTree, *AddrCache, *InvertedCache) {
	//init tree
	tree := NewSearchTree()
	stdlen := DEFAULT_SIZE
	clvdata := make(map[string]*SerializeObj)
	addrcache := InitAddrCache(addrCachesize)
	invtdcache := InitInvertedCache(invtdCachesize)
	for len(buffer) > 0 {
		tmp := buffer[:stdlen]
		objsize, _ := BytesToInt(tmp, false)
		objsize += uint64(stdlen)
		objbuff := buffer[stdlen:objsize]
		data, obj := decodeSerializeObj(objbuff)
		clvdata[data] = obj
		buffer = buffer[objsize:]
		tree.Insert(addrcache, invtdcache, raw, data, obj)
	}
	return tree, addrcache, invtdcache
}

func decodeSerializeObj(buffer []byte) (string, *SerializeObj) {
	stdlen := DEFAULT_SIZE
	freq,_ := BytesToInt(buffer[:FREQ_SIZE],false)
	buffer = buffer[FREQ_SIZE:]
	objaddrlen, _ := BytesToInt(buffer[:stdlen], false)
	buffer = buffer[stdlen:]
	var addrListEntry = new(AddrListEntry)
	if objaddrlen != 0 {
		size, _ := BytesToInt(buffer[:stdlen], false)
		off, _ := BytesToInt(buffer[stdlen:2*stdlen], false)
		addrListEntry.SetSize(uint64(size))
		addrListEntry.SetBlockoffset(uint64(off))
		buffer = buffer[2*stdlen:]
	}
	objinvtdlen, _ := BytesToInt(buffer[:stdlen], false)
	buffer = buffer[stdlen:]
	var invtdListEntry = new(InvertedListEntry)
	if objinvtdlen != 0 {
		size, _ := BytesToInt(buffer[:stdlen], false)
		mintime, _ := BytesToInt(buffer[stdlen:2*stdlen], false)
		maxtime, _ := BytesToInt(buffer[2*stdlen:3*stdlen], false)
		off, _ := BytesToInt(buffer[3*stdlen:4*stdlen], false)
		invtdListEntry.SetSize(uint64(size))
		invtdListEntry.SetMinTime(int64(mintime))
		invtdListEntry.SetMaxTime(int64(maxtime))
		invtdListEntry.SetBlockoffset(uint64(off))
		buffer = buffer[4*stdlen:]
	}
	data := string(buffer)
	obj := NewSerializeObj(data, uint32(freq),uint64(objaddrlen), addrListEntry, uint64(objinvtdlen), invtdListEntry)
	obj.UpdateSeializeObjSize()
	return data, obj
}
