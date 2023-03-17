package mpTrie

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"hash/crc32"
	"os"
	"sort"
)
var stdlen byte = DEFAULT_SIZE

//get more than qmin index grams,and write the file
func getIndexData(tree *gramIndex.IndexTree) (map[string]*SerializeObj, map[string]*InvertedListBlock, map[string][]*AddrCenterStatus) {
	res := make(map[string]*SerializeObj)
	res_invetedblk := make(map[string]*InvertedListBlock)
	res_addrCntStatus := make(map[string][]*AddrCenterStatus)
	var dfs func(node *gramIndex.IndexTreeNode, path []string)
	dfs = func(node *gramIndex.IndexTreeNode, path []string) {
		if node.Isleaf() == true {
			temp := ""
			for _, s := range path {
				temp += s
			}
			//process addr
			addrmp := node.AddrOffset()
			arrlen := uint64(len(addrmp))
			if arrlen != 0 {
				res_addrCntStatus[temp] = encodeGramAddrCntStatus(addrmp) //addrlistblock
			}

			//process inverted
			inverted := node.InvertedIndex()
			invtdlen := uint64(len(inverted))
			if invtdlen != 0 {
				res_invetedblk[temp] = encodeInvertedBlk(inverted) //inverted list block
			}
			//obj
			freq := node.Frequency()
			min, max := GetMaxAndMinTime(inverted)
			addrEntry := NewAddrListEntry(0)
			invertedEntry := NewInvertedListEntry(min, max, 0)
			obj := NewSerializeObj(temp,uint32(freq), arrlen, addrEntry, invtdlen, invertedEntry)
			res[temp] = obj
			//fmt.Println(temp, node.AddrOffset())
		}
		if len(node.Children()) == 0 {
			return
		}
		for _, child := range node.Children() {
			path = append(path, child.Data())
			dfs(child, path)
			path = path[:len(path)-1]
		}
	}
	root := tree.Root()
	path := make([]string, 0)
	dfs(root, path)
	return res, res_invetedblk, res_addrCntStatus
}
//todo check invertedlistblock convert hashcode
func HashInvertedBlk(invetdblk *InvertedListBlock) uint32 {
	blk := invetdblk.Mpblk()
	sidVec := make([]uint64,0)
	for key,_ := range blk{
		sidVec = append(sidVec,key)
	}
	res := make([]byte,0)
	for i:=0;i<len(sidVec);i++{
		sid := sidVec[i]
		timepoint := blk[sid]
		for _,point := range timepoint{
			idbyte, _ := IntToBytes(int(sid),DEFAULT_SIZE)
			timebyte, _ := IntToBytes(int(point.TimeStamp),DEFAULT_SIZE)
			fstpos,_ := IntToBytes(int((*point.Pos)[0]),2)
			res = append(res,idbyte...)
			res = append(res,timebyte...)
			res = append(res,fstpos...)
		}
	}
	hashcode := crc32.ChecksumIEEE(res)
	return hashcode
}

func SerializeGramIndexToFile(tree *gramIndex.IndexTree, filename string) {
	fb := make([]byte, 0)
	res, mp_invertedblk, res_addrctr := getIndexData(tree)
	var addrTotal uint64
	//1. serialize invertedlistblock
	ivtdIdxData := make([]string, 0)
	for data, _ := range mp_invertedblk {
		ivtdIdxData = append(ivtdIdxData, data)
	}
	sort.Strings(ivtdIdxData)
	invtdblkToOff := make(map[*InvertedListBlock]uint64, 0) //<invertedlistblock,offset> of the invetedlistblock
	hashToOff := make(map[uint32]uint64, 0)
	var start_invtblk uint64 = 0
	for _, data := range ivtdIdxData {
		invetdblk := mp_invertedblk[data]
		invtdblkToOff[invetdblk] = start_invtblk
		hash := HashInvertedBlk(invetdblk)
		hashToOff[hash] = start_invtblk
		tmpbytes := serializeInvertedListBlk(invetdblk)
		fb = append(fb, tmpbytes...)
		start_invtblk += invetdblk.Blksize() + DEFAULT_SIZE
	}

	//2. serialize encodeaddrblk
	addrIdxData := make([]string, 0)
	for data, _ := range res_addrctr {
		addrIdxData = append(addrIdxData, data)
	}
	sort.Strings(addrIdxData)
	//2.1 addrblk convert
	mp_addblk, addrblkToOff := addrCenterStatusToBLK(start_invtblk, addrIdxData, res_addrctr, hashToOff)
	for _, data := range addrIdxData {
		addrblk := mp_addblk[data]
		tmpbytes := serializeAddrListBlock(addrblk)
		fb = append(fb, tmpbytes...)
		addrTotal += addrblk.Blksize() + DEFAULT_SIZE
	}
	//3. serialize clv data block
	idxData := make([]string, 0)
	for data, _ := range res {
		idxData = append(idxData, data)
	}
	sort.Strings(idxData)

	for _, data := range idxData {
		obj := res[data]
		var addrblkoff, addrblksize, invtdblkroff, invtdblksize uint64
		if obj.InvertedListlen() == 0 {
			obj.InvertedListEntry().SetSize(0)
		} else if blk, ok := mp_invertedblk[data]; ok {
			invtdblkroff = invtdblkToOff[blk]
			invtdblksize = blk.Blksize()
			obj.InvertedListEntry().SetBlockoffset(invtdblkroff)
			obj.InvertedListEntry().SetSize(invtdblksize)
		}
		if obj.AddrListlen() == 0 {
			obj.AddrListEntry().SetSize(0)
		} else if blk, ok := mp_addblk[data]; ok {
			addrblkoff = addrblkToOff[blk]
			addrblksize = blk.Blksize()
			obj.AddrListEntry().SetBlockoffset(addrblkoff)
			obj.AddrListEntry().SetSize(addrblksize)
		}
		obj.UpdateSeializeObjSize()
		tmpbyte := serializeObj(obj)
		if tmpbyte == nil {
			fmt.Println(fmt.Errorf("the process of serialized obj had some error."))
			return
		}
		fb = append(fb, tmpbyte...)
	}

	//file tailer
	invtdTotalbyte, _ := IntToBytes(int(start_invtblk), stdlen)
	addrTotalbyte, _ := IntToBytes(int(addrTotal), stdlen)
	fb = append(fb, invtdTotalbyte...)
	fb = append(fb, addrTotalbyte...)

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	defer file.Close()
	if err != nil {
		fmt.Println("file open fail when mptrie serialize.", err)
		return
	}
	//fmt.Println(fb)
	_, err = file.Write(fb)
	if err != nil {
		fmt.Println("file write fail when mptrie serialize", err)
		return
	}

}
func serializeObj(obj *SerializeObj) []byte {
	res := make([]byte, 0)
	size, err := IntToBytes(int(obj.Size()), stdlen)
	res = append(res, size...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	freq,err := IntToBytes(int(obj.Freq()),FREQ_SIZE)
	res = append(res,freq...)
	if err !=nil{
		fmt.Println(err)
		return nil
	}
	addrlen, err := IntToBytes(int(obj.AddrListlen()), stdlen)
	res = append(res, addrlen...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if obj.AddrListlen() == 0 {
		//
	} else {
		//maybe don`t have record size
		entrysize, err := IntToBytes(int(obj.AddrListEntry().Size()), stdlen)
		res = append(res, entrysize...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		entryoff, err := IntToBytes(int(obj.AddrListEntry().Blockoffset()), stdlen)
		res = append(res, entryoff...)
		if err != nil {
			fmt.Println(err)
			return nil
		}

	}
	invtdlen, err := IntToBytes(int(obj.InvertedListlen()), stdlen)
	res = append(res, invtdlen...)
	if obj.InvertedListlen() == 0 {
		//
	} else {

		entrysize, err := IntToBytes(int(obj.InvertedListEntry().Size()), stdlen)
		res = append(res, entrysize...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		entryMinTime, err := IntToBytes(int(obj.InvertedListEntry().MinTime()), stdlen)
		res = append(res, entryMinTime...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		entryMaxTime, err := IntToBytes(int(obj.InvertedListEntry().MaxTime()), stdlen)
		res = append(res, entryMaxTime...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		entryoff, err := IntToBytes(int(obj.InvertedListEntry().Blockoffset()), stdlen)
		res = append(res, entryoff...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
	}
	res = append(res, []byte(obj.Data())...)
	return res
}