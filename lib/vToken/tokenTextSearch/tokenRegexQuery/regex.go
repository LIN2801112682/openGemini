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
package tokenRegexQuery

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenMatchQuery"
	"os"
	"regexp"
	"time"
)

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

func GetSuffixMap(re string, length int) map[string]struct{} {
	newRegex := re
	newRegex = RegexStandardization(newRegex)
	parseTree := GenerateParseTree(newRegex)
	nfa := GenerateNfa(parseTree)
	suffixMap := nfa.getSuffix(length)
	return suffixMap
}


func RegexSearch(re string, indexRoot *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache, tokenMap map[string][]*mpTrie.SearchTreeNode) map[utils.SeriesId]struct{} {
	fmt.Println("正则表达式:", re)
	var resArr = make(map[utils.SeriesId]struct{}, 0)
	q := 3
	start := time.Now().UnixMicro()
	//filter_start_time := time.Now().UnixMicro()
	suffixMap := GetSuffixMap(re, q)
	//filter_end_time := time.Now().UnixMicro()
	//fmt.Println("过滤时间:", filter_end_time-filter_start_time)
	isQ := true
	for key, _ := range suffixMap {
		if len(key) != q {
			isQ = false
			break
		}
	}
	//verification_time := int64(0)
	for fileId, _ := range filePtr {
		if !isQ {
			resArr = utils.Or(resArr, MatchWithoutGramMap(re, indexRoot, fileId, filePtr, addrCache, invertedCache))
		} else {
			//verification_start_time := time.Now().UnixMicro()
			resArr = utils.Or(resArr, MatchWithGramMap(tokenMap, suffixMap, re, indexRoot, fileId, filePtr, addrCache, invertedCache))
			//verification_end_time := time.Now().UnixMicro()
			//verification_time += verification_end_time - verification_start_time
		}
	}
	end := time.Now().UnixMicro()
	//fmt.Println("验证时间:", verification_time)
	fmt.Println("花费时间：", end-start)
	fmt.Println("结果条数：", len(resArr))
	return resArr
}

func MatchWithGramMap(gramMap map[string][]*mpTrie.SearchTreeNode, suffixMap map[string]struct{}, re string, indexRoot *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	regex, _ := regexp.Compile("^" + re + "$")
	sidmap := make(map[utils.SeriesId]struct{})
	//token_num := 0
	for suffix, _ := range suffixMap {
		positionList, find := gramMap[suffix]
		if find {
			//token_num += len(positionList)
			for i := 0; i < len(positionList); i++ {
				if regex.MatchString(positionList[i].Data()) {
					invertIndex1, invertIndex2, invertIndex3 := SearchString(positionList[i].Data(), indexRoot, fileId, filePtr, addrCache, invertedCache)
					for k, _ := range invertIndex1 {
						_, isfind := sidmap[k]
						if !isfind {
							sidmap[k] = struct{}{}
						}
					}
					for k, _ := range invertIndex2 {
						_, isfind := sidmap[k]
						if !isfind {
							sidmap[k] = struct{}{}
						}
					}
					for k, _ := range invertIndex3 {
						_, isfind := sidmap[k]
						if !isfind {
							sidmap[k] = struct{}{}
						}
					}
				}
			}
		}
	}
	//fmt.Println("一共有token数：", len(indexRoot.Children()))
	//fmt.Println("筛选后token数:", token_num)
	return sidmap
}

func MatchWithoutGramMap(re string, indexRoot *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	regex, _ := regexp.Compile("^" + re + "$")
	sidmap := make(map[utils.SeriesId]struct{})
	childrenlist := indexRoot.Children()
	for _, children := range childrenlist {
		label := children.Data()
		if regex.MatchString(label) {
			invertIndex1, invertIndex2, invertIndex3 := SearchString(label, indexRoot, fileId, filePtr, addrCache, invertedCache)
			for k, _ := range invertIndex1 {
				_, isfind := sidmap[k]
				if !isfind {
					sidmap[k] = struct{}{}
				}
			}
			for k, _ := range invertIndex2 {
				_, isfind := sidmap[k]
				if !isfind {
					sidmap[k] = struct{}{}
				}
			}
			for k, _ := range invertIndex3 {
				_, isfind := sidmap[k]
				if !isfind {
					sidmap[k] = struct{}{}
				}
			}
		}
	}
	return sidmap
}

func SearchString(label string, indexRoot *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) (utils.Inverted_index, utils.Inverted_index, utils.Inverted_index) {
	var invertIndexOffset uint64
	var addrOffset uint64
	var indexNode *mpTrie.SearchTreeNode
	var invertIndex1 utils.Inverted_index
	var invertIndex2 utils.Inverted_index
	var invertIndex3 utils.Inverted_index
	invertIndexOffset, addrOffset, indexNode = tokenMatchQuery.SearchNodeAddrFromPersistentIndexTree(fileId, []string{label}, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
	if len(indexNode.InvtdCheck()) > 0 {
		if _, ok := indexNode.InvtdCheck()[fileId]; ok {
			if indexNode.InvtdCheck()[fileId].Invtdlen() > 0 {
				invertIndex1 = mpTrie.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, fileId, filePtr, invertedCache)
			}
		}
	}
	invertIndex2 = mpTrie.SearchInvertedListFromChildrensOfCurrentNode(indexNode, invertIndex2, fileId, filePtr, addrCache, invertedCache)
	if len(indexNode.AddrCheck()) > 0 {
		if _, ok := indexNode.AddrCheck()[fileId]; ok {
			if indexNode.AddrCheck()[fileId].Addrlen() > 0 {
				addrOffsets := mpTrie.SearchAddrOffsetsFromCacheOrDisk(addrOffset, fileId, filePtr, addrCache)
				if indexNode != nil && len(addrOffsets) > 0 {
					invertIndex3 = mpTrie.TurnAddr2InvertLists(addrOffsets, fileId, filePtr, invertedCache)
				}
			}
		}
	}
	return invertIndex1, invertIndex2, invertIndex3
}
