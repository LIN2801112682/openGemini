/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package gramRegexQuery

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"os"
	"strings"
	"time"
)

type Regex struct {
	re   string
	gnfa *Gnfa
}

func NewRegex(re string, trietree *gramClvc.TrieTree) *Regex {
	parseTree := GenerateParseTree(re)
	nfa := GenerateNfa(parseTree)
	gnfa := GenerateGnfa(nfa, trietree)
	return &Regex{
		re:   re,
		gnfa: gnfa,
	}
}

type RegexPlus struct {
	re      string
	gnfa    *Gnfa
	front   uint8
	sidlist []*SeriesIdWithPosition
}

func NewRegexPlus(re string, front uint8) *RegexPlus {
	return &RegexPlus{re: re, front: front}
}

func (rp *RegexPlus) GenerateGNFA(trietree *gramClvc.TrieTree) {
	parseTree := GenerateParseTree(rp.re)
	nfa := GenerateNfa(parseTree)
	rp.gnfa = GenerateGnfa(nfa, trietree)
}

func GenerateRegexPlusList(re string, qmin int) (bool, []*RegexPlus) {
	result := make([]*RegexPlus, 0)
	relist := strings.Split(re, ".")
	if relist[len(relist)-1] == "" || relist[len(relist)-1] == "+" || relist[len(relist)-1] == "?" || relist[len(relist)-1] == "*" {
		relist = relist[:len(relist)-1]
	}
	i := 0
	if re[0] != '.' {
		regex := relist[i]
		if len(regex) < qmin {
			return false, make([]*RegexPlus, 0)
		}
		rp := NewRegexPlus(relist[i], ' ')
		result = append(result, rp)
		i = 1
	} else {
		relist = relist[1:]
	}
	for ; i < len(relist); i++ {
		ch := relist[i][0]
		if ch == '?' || ch == '+' || ch == '*' {
			regex := relist[i][1:]
			if len(regex) < qmin {
				return false, make([]*RegexPlus, 0)
			}
			rp := NewRegexPlus(regex, ch)
			result = append(result, rp)
		} else {
			regex := relist[i]
			if len(regex) < qmin {
				return false, make([]*RegexPlus, 0)
			}
			rp := NewRegexPlus(relist[i], '.')
			result = append(result, rp)
		}
	}
	return true, result
}

func RegexStandardization(re string) string {
	length := len(re)
	for i := 0; i < length; i++ {
		if (re[i] == '+' || re[i] == '?' || re[i] == '*') && re[i-1] != ')' {
			re = re[:i-1] + "(" + re[i-1:i] + ")" + re[i:]
			length += 2
			i += 2
		}
	}
	return re
}

func RegexSearch(re string, trietree *gramClvc.TrieTree, qmin int, index *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	fmt.Println("正则表达式:", re)
	start := time.Now().UnixMicro()
	var resArr = make(map[utils.SeriesId]struct{}, 0)
	for fileId, _ := range filePtr {
		resArr = utils.Or(resArr, RegexSearch1(re, trietree, qmin, index, fileId, filePtr, addrCache, invertedCache))
	}
	end := time.Now().UnixMicro()
	fmt.Println("总用时:", end-start)
	fmt.Println("结果数:", len(resArr))
	return resArr
}

func RegexSearch1(re string, trietree *gramClvc.TrieTree, qmin int, index *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	split := strings.Contains(re, ".")
	result := make(map[utils.SeriesId]struct{}, 0)
	if !split {
		sidmap := MatchRegex(re, trietree, index, fileId, filePtr, addrCache, invertedCache)
		for i := 0; i < len(sidmap); i++ {
			result[sidmap[i].sid] = struct{}{}
		}
	} else {
		isnoterror, rplist := GenerateRegexPlusList(re, qmin)
		if !isnoterror {
			//fmt.Println("syntax error !")
			return nil
		} else {
			sidmap := MatchRegexPlusList(rplist, trietree, index, fileId, filePtr, addrCache, invertedCache)
			for key := range sidmap {
				result[key] = struct{}{}
			}
		}
	}
	return result
}

func MatchRegex(re string, trietree *gramClvc.TrieTree, index *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) []*SeriesIdWithPosition {
	re = RegexStandardization(re)
	regex := NewRegex(re, trietree)
	regex.LoadInvertedIndex(index, fileId, filePtr, addrCache, invertedCache)
	sidlist := regex.Match()
	sidlist = SortAndRemoveDuplicate(sidlist)
	return sidlist
}

func MatchRegexPlusList(regexpluslist []*RegexPlus, trietree *gramClvc.TrieTree, indextree *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId][]uint16 {
	for i := 0; i < len(regexpluslist); i++ {
		regexpluslist[i].re = RegexStandardization(regexpluslist[i].re)
		regexpluslist[i].GenerateGNFA(trietree)
		regexpluslist[i].gnfa.LoadInvertedIndex(indextree, fileId, filePtr, addrCache, invertedCache)
		sidlist := regexpluslist[i].gnfa.Match()
		sidlist = SortAndRemoveDuplicate(sidlist)
		regexpluslist[i].sidlist = sidlist
	}
	resultlist := MergeRegexPlus(regexpluslist)
	return resultlist

}

func MergeRegexPlus(rplist []*RegexPlus) map[utils.SeriesId][]uint16 {
	// get start list
	mergemap := make(map[utils.SeriesId][]uint16)
	if rplist[0].front == '.' {
		for i := 0; i < len(rplist[0].sidlist); i++ {
			startpos := rplist[0].sidlist[i].startposition
			if startpos[0] == 0 {
				startpos = startpos[1:]
				if len(startpos) != 0 {
					mergemap[rplist[0].sidlist[i].sid] = rplist[0].sidlist[i].endposition
				}
			}

		}
	} else {
		for i := 0; i < len(rplist[0].sidlist); i++ {
			mergemap[rplist[0].sidlist[i].sid] = rplist[0].sidlist[i].endposition
		}
	}
	// recursion
	for i := 1; i < len(rplist); i++ {
		nextmergemap := make(map[utils.SeriesId][]uint16)
		for j := 0; j < len(rplist[i].sidlist); j++ {
			sid := rplist[i].sidlist[j].sid
			endlist, find := mergemap[sid]
			if find {
				canmerge, list := MergeWithOp(endlist, rplist[i].sidlist[j].startposition, rplist[i].sidlist[j].endposition, rplist[i].front)
				if canmerge {
					nextmergemap[sid] = list
				}
			}
		}
		mergemap = nextmergemap
	}
	return mergemap

}

func MergeWithOp(lastendposition []uint16, nextstartposition []uint16, nextendposition []uint16, op uint8) (bool, []uint16) {
	endposisiton := make([]uint16, 0)
	for i := 0; i < len(nextstartposition); i++ {
		for j := 0; j < len(lastendposition); j++ {
			if CanMergeWithOp(lastendposition[j], nextstartposition[i], op) {
				endposisiton = append(endposisiton, nextendposition[i])
				break
			}
		}
	}
	if len(endposisiton) == 0 {
		return false, endposisiton
	}
	return true, endposisiton
}

func CanMergeWithOp(lastend uint16, nextstart uint16, op uint8) bool {
	if op == '?' && nextstart-lastend <= 1 && nextstart-lastend >= 0 {
		return true
	} else if op == '+' && nextstart-lastend >= 1 {
		return true
	} else if op == '*' && nextstart-lastend >= 0 {
		return true
	} else if op == '.' && nextstart-lastend == 1 {
		return true
	} else {
		return false
	}
}

func (re *Regex) LoadInvertedIndex(index *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) {
	re.gnfa.LoadInvertedIndex(index, fileId, filePtr, addrCache, invertedCache)
}

func (re *Regex) Match() []*SeriesIdWithPosition {
	sidlist := re.gnfa.Match()
	return sidlist

}

func SortAndRemoveDuplicate(sidlist []*SeriesIdWithPosition) []*SeriesIdWithPosition {
	sidlist = RemoveDuplicate(sidlist)
	return sidlist
}
