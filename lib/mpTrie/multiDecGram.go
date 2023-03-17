package mpTrie

import (
	"io"
	"os"
)

//func NumberForFiles(files []string) (map[string]int, map[string]*os.File) {
//
//	return fileid,filesPtr
//}
func DecodeGramIndexFromMultiFile(files []string, addrCachesize, invtdCachesize int) (*SearchTree, map[int]*os.File, *AddrCache, *InvertedCache) {
	filesPtr := make(map[int]*os.File)
	//init tree,and addrCache,invtdCache
	searchtree := NewSearchTree()
	addrcache := InitAddrCache(addrCachesize)
	invtdcache := InitInvertedCache(invtdCachesize)
	for fileid,filename := range files{
		ptr, filesize := GetFilePiont(filename)
		filesPtr[fileid+1] = ptr
		unserializeGramIndexFromMultiFiles(searchtree,ptr,fileid+1,filesize,addrcache,invtdcache)
	}
	return searchtree,filesPtr,addrcache,invtdcache
}

func unserializeGramIndexFromMultiFiles(searchtree *SearchTree,file *os.File,fileid int, filesize int64,addrcache *AddrCache, invtdcache *InvertedCache){
	if file==nil {
		return
	}
	var invtdTotal,addrTotal uint64
	invtdTotalbyte := ReadByteFormFilePtr(file,filesize-2*DEFAULT_SIZE,filesize-DEFAULT_SIZE,io.SeekStart)
	if invtdTotalbyte!=nil{
		invtdTotal, _ = BytesToInt(invtdTotalbyte, true)
	}
	addrTotalbyte := ReadByteFormFilePtr(file, filesize-DEFAULT_SIZE,filesize,io.SeekStart)
	if addrTotalbyte!=nil{
		addrTotal, _ = BytesToInt(addrTotalbyte, true)
	}
	clvdataStart := int64(invtdTotal + addrTotal)
	clvdatabuf := ReadByteFormFilePtr(file,clvdataStart,filesize-2*DEFAULT_SIZE,io.SeekStart)

	//decode obj
	unserializeObjGramFromMultiFiles(searchtree,clvdatabuf, file,fileid,addrcache, invtdcache)

}
func unserializeObjGramFromMultiFiles(searchtree *SearchTree,buffer []byte, file *os.File,fileid int,addrcache *AddrCache, invtdcache *InvertedCache){
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
		searchtree.InsertGramForMulti(addrcache, invtdcache, fileid,file, data, obj)
	}
}
