package mpTrie

func UnserializeTokenIndexFromFile(buffer []byte, filesize int64, addrCachesize, invtdCachesize int) (*SearchTree, *AddrCache, *InvertedCache) {
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
	tree, addrcache, invtdcache := unserializeTokenObj(clvdatabuf, raw, addrCachesize, invtdCachesize)
	return tree, addrcache, invtdcache
}

func unserializeTokenObj(buffer, raw []byte, addrCachesize, invtdCachesize int) (*SearchTree, *AddrCache, *InvertedCache) {
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
		tree.InsertToken(addrcache, invtdcache, raw, data, obj)
	}
	return tree, addrcache, invtdcache
}
