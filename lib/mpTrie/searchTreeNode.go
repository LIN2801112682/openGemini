package mpTrie

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
	"os"
	"sort"
)

type SearchTreeNode struct {
	data       string
	freq       int
	children   map[int]*SearchTreeNode
	addrCheck  map[int]*AddrInfo
	invtdCheck map[int]*InvtdInfo
	//prefixGrams []utils.FuzzyPrefixGram
	isleaf bool
}
//
//func (node *SearchTreeNode) PrefixGrams() []utils.FuzzyPrefixGram {
//	return node.prefixGrams
//}
//
//func (node *SearchTreeNode) SetPrefixGrams(prefixGrams []utils.FuzzyPrefixGram) {
//	node.prefixGrams = prefixGrams
//}

func NewSearchTreeNode(data string) *SearchTreeNode {
	return &SearchTreeNode{
		data:       data,
		freq:       0,
		children:   make(map[int]*SearchTreeNode),
		addrCheck:  make(map[int]*AddrInfo),
		invtdCheck: make(map[int]*InvtdInfo),
		//prefixGrams: make([]utils.FuzzyPrefixGram, 0),
		isleaf: false,
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

/*func (node *SearchTreeNode) TokenPrefixGrams(prefixlen, distance int)  {
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
				node.children[i].SetPrefixGrams(qgramData[:prefixlen*distance+1])
			}
		}
	}
}*/

func (node *SearchTreeNode) printsearchTreeNode(file map[int]*os.File, level int, addrcache *AddrCache, invtdcache *InvertedCache) {
	fmt.Println()
	for i := 0; i < level; i++ {
		fmt.Print("      ")
	}
	fmt.Print(node.data, " - ", node.freq, " - ", node.isleaf) //, " -prefixGram: ", node.PrefixGrams(), " - "
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

func GramPrefixGrams(str string, prefixlen int, distance int) []utils.FuzzyPrefixGram {
	qgramData := make([]utils.FuzzyPrefixGram, 0)
	lenData := len(str)
	for m := 0; m < lenData-prefixlen+1; m++ {
		qgramData = append(qgramData, utils.NewFuzzyPrefixGram(str[m:m+prefixlen], int8(m)))
	}
	sort.SliceStable(qgramData, func(m, n int) bool {
		if qgramData[m].Gram() <= qgramData[n].Gram() {
			return true
		}
		return false
	})
	return qgramData[:prefixlen*distance+1]
}

func (node *SearchTreeNode) GeneratePrefixIndex(prefixLen int, distance int) (map[int]map[string]struct{}, map[string]map[int]map[utils.FuzzyPrefixGram]struct{}) {
	limitLen := prefixLen*distance + 1 + prefixLen - 1
	shortIndex := make(map[int]map[string]struct{})
	longIndex := make(map[string]map[int]map[utils.FuzzyPrefixGram]struct{})
	flagIndex := make(map[string]struct{})
	for childIndex, _ := range node.Children() {
		substring := node.Children()[childIndex].Data()
		if len(substring) >= limitLen {
			_, ok1 := flagIndex[substring]
			if ok1 {
				continue
			} else {
				flagIndex[substring] = struct{}{}
				qgramData := GramPrefixGrams(substring, prefixLen, distance)
				for n := 0; n < len(qgramData); n++ {
					longIndexData := utils.NewFuzzyPrefixGram(substring, qgramData[n].Pos())
					_, ok2 := longIndex[qgramData[n].Gram()]
					if !ok2 {
						longIndexString := make(map[int]map[utils.FuzzyPrefixGram]struct{})
						longIndexString[len(substring)] = make(map[utils.FuzzyPrefixGram]struct{})
						longIndexString[len(substring)][longIndexData] = struct{}{}
						longIndex[qgramData[n].Gram()] = longIndexString
					} else {
						_, ok3 := longIndex[qgramData[n].Gram()][len(substring)]
						if !ok3 {
							longIndex[qgramData[n].Gram()][len(substring)] = make(map[utils.FuzzyPrefixGram]struct{})
							longIndex[qgramData[n].Gram()][len(substring)][longIndexData] = struct{}{}
						} else {
							longIndex[qgramData[n].Gram()][len(substring)][longIndexData] = struct{}{}
						}
					}
				}
			}
		} else {
			_, ok4 := shortIndex[len(substring)]
			if !ok4 {
				shortMap := make(map[string]struct{})
				shortMap[substring] = struct{}{}
				shortIndex[len(substring)] = shortMap
			} else {
				shortIndex[len(substring)][substring] = struct{}{}
			}
		}

	}
	return shortIndex, longIndex
}
func (node *SearchTreeNode) FuzzyGramGeneratePrefixIndex(logString []string, prefixLen int, distance int, qmin int, qmax int) (map[int]map[string]struct{}, map[string]map[int]map[utils.FuzzyPrefixGram]struct{}) {
	limitLen := prefixLen*distance + 1 + prefixLen - 1
	shortIndex := make(map[int]map[string]struct{})
	longIndex := make(map[string]map[int]map[utils.FuzzyPrefixGram]struct{})
	flagIndex := make(map[string]struct{})
	for logIndex := range logString {
		str := logString[logIndex]
		if len(str) >= qmax {
			for k := qmin; k <= qmax; k++ {
				for i := 0; i < len(str)-k+1; i++ {
					substring := str[i : i+k]
					if len(substring) >= limitLen {
						_, ok1 := flagIndex[substring]
						if ok1 {
							continue
						} else {
							flagIndex[substring] = struct{}{}
							qgramData := GramPrefixGrams(substring, prefixLen, distance)
							for n := 0; n < len(qgramData); n++ {
								longIndexData := utils.NewFuzzyPrefixGram(substring, qgramData[n].Pos())
								_, ok2 := longIndex[qgramData[n].Gram()]
								if !ok2 {
									longIndexString := make(map[int]map[utils.FuzzyPrefixGram]struct{})
									longIndexString[len(substring)] = make(map[utils.FuzzyPrefixGram]struct{})
									longIndexString[len(substring)][longIndexData] = struct{}{}
									longIndex[qgramData[n].Gram()] = longIndexString
								} else {
									_, ok3 := longIndex[qgramData[n].Gram()][len(substring)]
									if !ok3 {
										longIndex[qgramData[n].Gram()][len(substring)] = make(map[utils.FuzzyPrefixGram]struct{})
										longIndex[qgramData[n].Gram()][len(substring)][longIndexData] = struct{}{}
									} else {
										longIndex[qgramData[n].Gram()][len(substring)][longIndexData] = struct{}{}
									}
								}
							}
						}
					} else {
						_, ok4 := shortIndex[len(substring)]
						if !ok4 {
							shortMap := make(map[string]struct{})
							shortMap[substring] = struct{}{}
							shortIndex[len(substring)] = shortMap
						} else {
							shortIndex[len(substring)][substring] = struct{}{}
						}
					}
				}
			}
		} else {
			for k := qmin; k <= len(str); k++ {
				for j := 0; j < len(str)-k+1; j++ {
					substring := str[j : j+k]
					if len(substring) >= limitLen {
						_, ok1 := flagIndex[substring]
						if ok1 {
							continue
						} else {
							flagIndex[substring] = struct{}{}
							qgramData := GramPrefixGrams(substring, prefixLen, distance)
							for n := 0; n < len(qgramData); n++ {
								longIndexData := utils.NewFuzzyPrefixGram(substring, qgramData[n].Pos())
								_, ok2 := longIndex[qgramData[n].Gram()]
								if !ok2 {
									longIndexString := make(map[int]map[utils.FuzzyPrefixGram]struct{})
									longIndexString[len(substring)] = make(map[utils.FuzzyPrefixGram]struct{})
									longIndexString[len(substring)][longIndexData] = struct{}{}
									longIndex[qgramData[n].Gram()] = longIndexString
								} else {
									_, ok3 := longIndex[qgramData[n].Gram()][len(substring)]
									if !ok3 {
										longIndex[qgramData[n].Gram()][len(substring)] = make(map[utils.FuzzyPrefixGram]struct{})
										longIndex[qgramData[n].Gram()][len(substring)][longIndexData] = struct{}{}
									} else {
										longIndex[qgramData[n].Gram()][len(substring)][longIndexData] = struct{}{}
									}
								}
							}
						}
					} else {
						_, ok4 := shortIndex[len(substring)]
						if !ok4 {
							shortMap := make(map[string]struct{})
							shortMap[substring] = struct{}{}
							shortIndex[len(substring)] = shortMap
						} else {
							shortIndex[len(substring)][substring] = struct{}{}
						}
					}
				}
			}
		}
	}

	sumIndex := 0
	sumIndex = sumIndex + len(flagIndex)
	for _, value := range shortIndex {
		sumIndex = sumIndex + len(value)
	}
	return shortIndex, longIndex
}
