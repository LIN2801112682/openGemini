package mpTrie

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
	"os"
	"sort"
)

type SearchTreeNode struct {
	data        string
	freq        int
	children    map[int]*SearchTreeNode
	addrCheck   map[int]*AddrInfo
	invtdCheck  map[int]*InvtdInfo //这两个int 都是fileId
	prefixGrams []utils.FuzzyPrefixGram
	isleaf      bool
}

func (node *SearchTreeNode) PrefixGrams() []utils.FuzzyPrefixGram {
	return node.prefixGrams
}

func (node *SearchTreeNode) SetPrefixGrams(prefixGrams []utils.FuzzyPrefixGram) {
	node.prefixGrams = prefixGrams
}

func NewSearchTreeNode(data string) *SearchTreeNode {
	return &SearchTreeNode{
		data:        data,
		freq: 0,
		children:    make(map[int]*SearchTreeNode),
		addrCheck:   make(map[int]*AddrInfo),
		invtdCheck:  make(map[int]*InvtdInfo),
		prefixGrams: make([]utils.FuzzyPrefixGram, 0),
		isleaf:      false,
	}
}

func (node *SearchTreeNode) Data() string {
	return node.data
}

func (node *SearchTreeNode) SetData(data string) {
	node.data = data
}

func (node *SearchTreeNode) Freq() int {
	return node.freq
}

func (node *SearchTreeNode) SetFreq(freq int) {
	node.freq = freq
}

func (node *SearchTreeNode) Children() map[int]*SearchTreeNode {
	return node.children
}

func (node *SearchTreeNode) SetChildren(children map[int]*SearchTreeNode) {
	node.children = children
}

func (node *SearchTreeNode) AddrCheck() map[int]*AddrInfo {
	return node.addrCheck
}

func (node *SearchTreeNode) SetAddrCheck(addrCheck map[int]*AddrInfo) {
	node.addrCheck = addrCheck
}

func (node *SearchTreeNode) InvtdCheck() map[int]*InvtdInfo {
	return node.invtdCheck
}

func (node *SearchTreeNode) SetInvtdCheck(invtdCheck map[int]*InvtdInfo) {
	node.invtdCheck = invtdCheck
}

func (node *SearchTreeNode) Isleaf() bool {
	return node.isleaf
}

func (node *SearchTreeNode) SetIsleaf(isleaf bool) {
	node.isleaf = isleaf
}

type AddrInfo struct {
	addrlen       int
	addrblkOffset uint64
	addrblksize   uint64
}

func NewAddrInfo(addrlen int, addrblkOffset uint64, addrblksize uint64) *AddrInfo {
	return &AddrInfo{addrlen: addrlen, addrblkOffset: addrblkOffset, addrblksize: addrblksize}
}

func (a *AddrInfo) Addrlen() int {
	return a.addrlen
}

func (a *AddrInfo) SetAddrlen(addrlen int) {
	a.addrlen = addrlen
}

func (a *AddrInfo) AddrblkOffset() uint64 {
	return a.addrblkOffset
}

func (a *AddrInfo) SetAddrblkOffset(addrblkOffset uint64) {
	a.addrblkOffset = addrblkOffset
}

func (a *AddrInfo) Addrblksize() uint64 {
	return a.addrblksize
}

func (a *AddrInfo) SetAddrblksize(addrblksize uint64) {
	a.addrblksize = addrblksize
}

type InvtdInfo struct {
	invtdlen      int
	ivtdblkOffset uint64
	ivtdblksize   uint64
}

func NewInvtdInfo(invtdlen int, ivtdblkOffset uint64, ivtdblksize uint64) *InvtdInfo {
	return &InvtdInfo{invtdlen: invtdlen, ivtdblkOffset: ivtdblkOffset, ivtdblksize: ivtdblksize}
}

func (i *InvtdInfo) Invtdlen() int {
	return i.invtdlen
}

func (i *InvtdInfo) SetInvtdlen(invtdlen int) {
	i.invtdlen = invtdlen
}

func (i *InvtdInfo) IvtdblkOffset() uint64 {
	return i.ivtdblkOffset
}

func (i *InvtdInfo) SetIvtdblkOffset(ivtdblkOffset uint64) {
	i.ivtdblkOffset = ivtdblkOffset
}

func (i *InvtdInfo) Ivtdblksize() uint64 {
	return i.ivtdblksize
}

func (i *InvtdInfo) SetIvtdblksize(ivtdblksize uint64) {
	i.ivtdblksize = ivtdblksize
}

func (node *SearchTreeNode) GetGramMap(q int) map[string][]*SearchTreeNode {
	result := make(map[string][]*SearchTreeNode)
	childrenlist := node.Children()
	for _, children := range childrenlist {
		label := children.Data()
		for j := 0; j < len(label)-q+1; j++ {
			positionList, find := result[label[j:j+q]]
			if !find {
				pl := make([]*SearchTreeNode, 1)
				pl[0] = children
				result[label[j:j+q]] = pl
			} else {
				positionList = append(positionList, children)
				result[label[j:j+q]] = positionList
			}
		}
	}
	return result
}

func (node *SearchTreeNode) TokenPrefixGrams(prefixlen, distance int)  {
	prefixgramcount := prefixlen * distance + 1
	for i, _ := range node.Children() {
		qgramData := make([]utils.FuzzyPrefixGram, 0)
		lenChildrendata := len(node.Children()[i].Data())
		if lenChildrendata-prefixlen+1 < prefixgramcount {
			continue
		} else {
			for m := 0; m < lenChildrendata-prefixlen+1; m++ {
				qgramData = append(qgramData, utils.NewFuzzyPrefixGram(node.Children()[i].Data()[m:m+prefixlen], int8(m)))
			}
			sort.SliceStable(qgramData, func(m, n int) bool {
				if qgramData[m].Gram() <= qgramData[n].Gram() {
					return true
				}
				return false
			})
			if len(qgramData) > 0 {
				node.children[i].SetPrefixGrams(qgramData[:lenChildrendata-prefixlen+1])
			}
		}
	}
}

func (node *SearchTreeNode) printsearchTreeNode(file map[int]*os.File, level int, addrcache *AddrCache, invtdcache *InvertedCache) {
	fmt.Println()
	for i := 0; i < level; i++ {
		fmt.Print("      ")
	}
	fmt.Print(node.data, " - ", node.freq, " - ", node.isleaf, " -prefixGram: ", node.PrefixGrams(), " - ")
	for fileid, addrInfo := range node.AddrCheck() {
		addrblk := addrcache.Get(addrInfo.addrblkOffset, fileid)
		if addrblk != nil && addrInfo.addrlen != 0 {
			blk := addrblk.Mpblk()
			fmt.Print(" -addr:  <", fileid, ">")
			for data, off := range blk {
				listBlk := UnserializeInvertedListBlk(data, file[fileid])
				fmt.Print(listBlk.Mpblk())
				fmt.Print(",", off)
			}

		} else {
			fmt.Print(" - ", addrInfo.addrblkOffset)
		}
	}

	for fileid, invtdInfo := range node.InvtdCheck() {
		invtdblk := invtdcache.Get(invtdInfo.ivtdblkOffset, fileid)
		if invtdblk != nil && invtdInfo.invtdlen != 0 {
			blk := invtdblk.Mpblk()
			fmt.Print(" -invt: <", fileid, ">")
			//obj.PrintInvertedBlk(blk)
			fmt.Print(blk)
		} else {
			fmt.Print(" - ", invtdInfo.ivtdblkOffset)
		}
	}

	for _, node := range node.children {
		node.printsearchTreeNode(file, level+1, addrcache, invtdcache)
	}
}
