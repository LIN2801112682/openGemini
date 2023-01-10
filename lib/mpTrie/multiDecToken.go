package mpTrie

import (
	"io"
	"os"
)

//func NumberForFiles(files []string) (map[string]int, map[string]*os.File) {
//
//	return fileid,filesPtr
//}

/*func readTokenFuzzyPrefixFromFile(fileIdMp map[string]int, searchtree *SearchTree, fuzzyfile string) {
	children := searchtree.Root().Children()
	file, err := os.Open(fuzzyfile)
	if err != nil {
		fmt.Println(fmt.Errorf("readTokenFuzzyPrefixFromFile : open file failed."))
	}
	reader := bufio.NewReader(file)
	for {
		unit, errinfo := reader.ReadString('<')
		if errinfo != nil && errinfo != io.EOF {
			fmt.Println(fmt.Errorf("readTokenFuzzyPrefixFromFile : read file failed."))
		}
		if errinfo == io.EOF {
			break
		}
		unit = unit[:len(unit)-1]
		unitsplit := strings.Split(unit, ",")
		filename := unitsplit[0]
		fileid := fileIdMp[filename]
		last := unitsplit[1]
		last = strings.TrimSpace(last)
		items := strings.Split(last, "#")
		for _, item := range items {
			if item == "" {
				continue
			}
			one := strings.Split(item, "$")
			datahash := utils.StringToHashCode(one[0])
			tmp := []byte(one[1])
			fuzzyPrefix := make([]utils.FuzzyPrefixGram, 0)
			decoder := gob.NewDecoder(bytes.NewBuffer(tmp))
			err := decoder.Decode(&fuzzyPrefix)
			if err != nil {
				return
			}
			if node, ok := children[datahash]; ok {
				node.AddPrefixGrams(fuzzyPrefix, fileid)
			} else {
				fmt.Println(fmt.Errorf("readTokenFuzzyPrefixFromFile : fuzzyFile has some diff data. "))
				return
			}
		}
	}
}*/

func DecodeTokenIndexFromMultiFile(files []string, addrCachesize, invtdCachesize int) (*SearchTree, map[int]*os.File, *AddrCache, *InvertedCache) {
	searchtree := NewSearchTree()
	addrcache := InitAddrCache(addrCachesize)
	invtdcache := InitInvertedCache(invtdCachesize)
	fileIdmp := make(map[string]int)
	filesPtr := make(map[int]*os.File)
	for id, filename := range files {
		ptr, filesize := GetFilePiont(filename)
		fileid := id + 1
		filesPtr[fileid] = ptr
		fileIdmp[filename] = fileid
		UnserializeTokenIndexFromMultiFiles(searchtree, ptr, fileid, filesize, addrcache, invtdcache)
	}
	//readTokenFuzzyPrefixFromFile(fileIdmp, searchtree, fuzzyfile)
	return searchtree, filesPtr, addrcache, invtdcache
}

func UnserializeTokenIndexFromMultiFiles(searchtree *SearchTree, file *os.File, fileid int, filesize int64, addrcache *AddrCache, invtdcache *InvertedCache) {
	if file == nil {
		return
	}
	var invtdTotal, addrTotal uint64
	invtdTotalbyte := ReadByteFormFilePtr(file, filesize-2*DEFAULT_SIZE, filesize-DEFAULT_SIZE, io.SeekStart)
	if invtdTotalbyte != nil {
		invtdTotal, _ = BytesToInt(invtdTotalbyte, true)
	}
	addrTotalbyte := ReadByteFormFilePtr(file, filesize-DEFAULT_SIZE, filesize, io.SeekStart)
	if addrTotalbyte != nil {
		addrTotal, _ = BytesToInt(addrTotalbyte, true)
	}
	clvdataStart := int64(invtdTotal + addrTotal)
	clvdatabuf := ReadByteFormFilePtr(file, clvdataStart, filesize-2*DEFAULT_SIZE, io.SeekStart)
	//decode obj
	unserializeObjTokenFromMultiFiles(searchtree, clvdatabuf, file, fileid, addrcache, invtdcache)

}
func unserializeObjTokenFromMultiFiles(searchtree *SearchTree, buffer []byte, file *os.File, fileid int, addrcache *AddrCache, invtdcache *InvertedCache) {
	stdlen := DEFAULT_SIZE
	clvdata := make(map[string]*SerializeObj)
	for len(buffer) > 0 {
		tmp := buffer[:stdlen]
		objsize, _ := BytesToInt(tmp, false)
		objsize += uint64(stdlen)
		objbuff := buffer[stdlen:objsize]
		data, obj := decodeSerializeObj(objbuff)
		clvdata[data] = obj
		buffer = buffer[objsize:]
		searchtree.InsertTokenForMulti(addrcache, invtdcache, fileid, file, data, obj)
	}
}
