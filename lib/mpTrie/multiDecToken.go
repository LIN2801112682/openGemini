package mpTrie

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"unsafe"
)


var MEMORY float64
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
		fmt.Println("************************************",id,"*******************************")
		UnserializeTokenIndexFromMultiFiles(searchtree, ptr, fileid, filesize, addrcache, invtdcache)
	}
	treesize := sizeofDiyTreeNode(searchtree.root)
	fmt.Printf("sizeDiy of searchTree:%fGBytes,%fMBytes\n",float64(treesize)/(1024*1024*1024),float64(treesize)/(1024*1024))

	return searchtree, filesPtr, addrcache, invtdcache
}

func sizeofDiyTreeNode(node *SearchTreeNode) int{
	if node==nil{
		return int(unsafe.Sizeof(node))
	}
	addrInfoSize,invtdInfoSize := 8*3,8*3
	addchecksize := len(node.addrCheck)*(8+8+addrInfoSize) //keySize+valueSize
	invtdchecksize := len(node.invtdCheck)*(8+8+invtdInfoSize)
	size :=int(unsafe.Sizeof(node.data) + unsafe.Sizeof(node.freq)+unsafe.Sizeof(node.isleaf) + unsafe.Sizeof(node.addrCheck) +unsafe.Sizeof(node.invtdCheck) + unsafe.Sizeof(node.children))+addchecksize+invtdchecksize

	for k,v := range node.children{
		tmpSize := sizeofDiyTreeNode(v)
		size += int(unsafe.Sizeof(v)+unsafe.Sizeof(k))+tmpSize+8
	}
	return size
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
		before := TraceMemStats_GetMemory()
		//fmt.Println("----------------------------------unserializeObjTokenFromMultiFiles-------------------------------------------------------")
		searchtree.InsertTokenForMulti(addrcache, invtdcache, fileid, file, data, obj)
		after := TraceMemStats_GetMemory()
		MEMORY += after-before
	}
}


func sizeDiyInvertedCache(cache *InvertedCache) int{
	//unsafe.Sizeof(hmap)+len(map)*8*(unsafe.Sizeof(key)+unsafe.Sizeof(value))
	if cache==nil{
		return int(unsafe.Sizeof(cache))
	}
	size := int(unsafe.Sizeof(cache.capicity)+unsafe.Sizeof(cache.used)+unsafe.Sizeof(cache.blkcache)+unsafe.Sizeof(cache.head)+unsafe.Sizeof(cache.tail))
	for k,v := range cache.Blkcache(){
		tmpsize := sizeofInvertedNode(v)
		size += int(unsafe.Sizeof(k))+tmpsize
	}
	size += sizeofInvertedNode(cache.head)+sizeofInvertedNode(cache.tail)
	return size
}

func sizeofInvertedNode(node *InvertedNode)int{
	if node==nil{
		return int(unsafe.Sizeof(node))
	}
	//param is double ptr
	size := 16+int(unsafe.Sizeof(node.key)+unsafe.Sizeof(node.value))
	//invertedlistBlock size
	var blksize int
	if node.value!=nil{
		itemslice := node.value.blk
		blksize += 8
		for _,item := range itemslice{
			blksize += 8+8+len(item.PosBlock())*2+8
		}
		for k,v := range node.value.mpblk{
			blksize +=  int(unsafe.Sizeof(k))+len(v)*2
		}
	}
	nextsize := sizeofInvertedNode(node.next)
	size += blksize + nextsize
	return size
}

func sizeDiyAddrCache(cache *AddrCache) int{
	//unsafe.Sizeof(hmap)+len(map)*8*(unsafe.Sizeof(key)+unsafe.Sizeof(value))
	if cache==nil{
		return int(unsafe.Sizeof(cache))
	}
	size := int(unsafe.Sizeof(cache.capicity)+unsafe.Sizeof(cache.used)+unsafe.Sizeof(cache.blk))
	size += sizeofAddrNode(cache.list)+sizeofAddrNode(cache.head)+sizeofAddrNode(cache.tail)
	for k,node := range cache.blk{
		size += 8*(int(unsafe.Sizeof(k)+unsafe.Sizeof(node))+sizeofAddrNode(node))
	}
	return size
}
func sizeofAddrNode(node *AddrNode)int{
	if node==nil{
		return int(unsafe.Sizeof(node))
	}
	//8:ptr
	size := 8+int(unsafe.Sizeof(node.key)+unsafe.Sizeof(node.value))
	addritemSize := 8+2+8+8 //ptr+addrdata_uint64+indexEntryOffset_uint16+size_uint64
	//sum addrlistBlock size
	if node.value!=nil{
		size+=len(node.value.blk)*addritemSize+len(node.value.mpblk)*(8+2)+8
	}
	size += sizeofAddrNode(node.next)
	return size
}

func TraceMemStats() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	var unit = 1024*1024*1024
	log.Printf("Alloc:%f(Gbytes) HeapIdle:%f(Gbytes) HeapReleased:%f(Gbytes)", float64(ms.Alloc)/float64(unit), float64(ms.HeapIdle)/float64(unit), float64(ms.HeapReleased)/float64(unit))
}
func TraceMemStats_GetMemory() float64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	var unit = 1024*1024*1024
	return float64(ms.Alloc)/float64(unit)
}