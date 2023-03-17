package mpTrie

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
	"os"
	"sort"
	"strings"
)

/**
* @ Author: Yaixihn
* @ Dec:map+trie落盘
* @ Date: 2022/9/18 13:40
*/

//func writeTokenFuzzyPrefixToFile(tree *tokenIndex.IndexTree, fuzzyfile, filename string) error {
//	root := tree.Root()
//	fb := make([]byte,0)
//	file, err := os.OpenFile(fuzzyfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
//	if err != nil {
//		return err
//	}
//	file.WriteString(filename+",")
//	for _,token := range  root.Children(){
//		data := token.Data()
//		gram := token.PrefixGram()
//		if gram!=nil{
//			fb = append(fb,[]byte(data)...)
//			fb = append(fb,'$')
//			buf := new(bytes.Buffer)
//			encoder := gob.NewEncoder(buf)
//			err := encoder.Encode(gram)
//			if err != nil {
//				return fmt.Errorf("encode TokenFuzzyPrefix is failed.")
//			}
//			fb = append(fb, buf.Bytes()...)
//			fb = append(fb, '#')
//		}
//	}
//	_, err = file.Write(fb)
//	if err != nil {
//		return fmt.Errorf("file write fail when fuzzyPrefixGram serialize")
//	}
//	file.WriteString("<")
//	return nil
//}

func SerializeTokenIndexToFile(tree *tokenIndex.IndexTree, filename string) {
	//err := writeTokenFuzzyPrefixToFile(tree, fuzzyfile, filename)
	//if err != nil {
	//	fmt.Println(err)
	//}
	fb := make([]byte, 0)
	res, mp_invertedblk, res_addrctr := getIndexTokenData(tree)
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


//get more than qmin index grams,and write the file
func getIndexTokenData(tree *tokenIndex.IndexTree) (map[string]*SerializeObj, map[string]*InvertedListBlock, map[string][]*AddrCenterStatus) {
	res := make(map[string]*SerializeObj)
	res_invetedblk := make(map[string]*InvertedListBlock)
	res_addrCntStatus := make(map[string][]*AddrCenterStatus)
	var dfs func(node *tokenIndex.IndexTreeNode, path []string)
	dfs = func(node *tokenIndex.IndexTreeNode, path []string) {
		if node.Isleaf() == true {
			temp := ""
			for _, s := range path {
				temp += s + " "
			}
			temp = strings.TrimSpace(temp)
			//process addr
			addrmp := node.AddrOffset()
			arrlen := uint64(len(addrmp))
			if arrlen != 0 {
				res_addrCntStatus[temp] = encodeTokenAddrCntStatus(addrmp) //addrlistblock
			}

			//process inverted
			inverted := node.InvertedIndex()
			invtdlen := uint64(len(inverted))
			if invtdlen != 0 {
				res_invetedblk[temp] = encodeInvertedBlk(inverted) //inverted list block
			}
			//obj
			freq := uint32(node.Frequency())
			min, max := GetMaxAndMinTime(inverted)
			addrEntry := NewAddrListEntry(0)
			invertedEntry := NewInvertedListEntry(min, max, 0)
			obj := NewSerializeObj(temp, freq,arrlen, addrEntry, invtdlen, invertedEntry)
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

