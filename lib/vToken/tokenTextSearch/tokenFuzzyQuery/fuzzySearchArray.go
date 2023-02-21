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
package tokenFuzzyQuery

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenMatchQuery"
	"os"
	"sort"
	"time"
)

func AbsInt(a int8) int8 {
	if a >= 0 {
		return a
	} else {
		return -a
	}
}
func MinThree(a, b, c int) int {
	var min int
	if a >= b {
		min = b
	} else {
		min = a
	}
	if min >= c {
		return c
	} else {
		return min
	}
}
func minDistanceToken(word1 string, word2 string) int {
	l1 := len(word1)
	l2 := len(word2)

	dp := make([][]int, l1+1)

	dp[0] = make([]int, l2+1)
	for j := 0; j <= l2; j++ {
		dp[0][j] = j
	}
	for i := 1; i <= l1; i++ {
		dp[i] = make([]int, l2+1)
		dp[i][0] = i
		for j := 1; j <= l2; j++ {
			if word1[i-1:i] == word2[j-1:j] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = MinThree(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]) + 1
			}
		}
	}
	return dp[l1][l2]
}

func VerifyED(searStr string, dataStr string, distance int) bool {
	if minDistanceToken(searStr, dataStr) <= distance {
		return true
	} else {
		return false
	}
}

func UnionMapPos(map1 *[]utils.SeriesId, mapPos map[utils.SeriesId][]uint16) *[]utils.SeriesId {
	if len(mapPos) == 0 {
		return map1
	}
	for key, _ := range mapPos {
		*map1=append(*map1, key)
	}
	return map1
}
func FuzzySearchChildrens(indexNode *mpTrie.SearchTreeNode, resMapFuzzy *[]utils.SeriesId, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) *[]utils.SeriesId {
	if indexNode != nil {
		for _, child := range indexNode.Children() {
			if len(child.InvtdCheck()) > 0 {
				if _, ok := child.InvtdCheck()[fileId]; ok {
					if child.InvtdCheck()[fileId].Invtdlen() > 0 {
						childInvertIndexOffset := child.InvtdCheck()[fileId].IvtdblkOffset()
						childInvertedIndex := make(map[utils.SeriesId][]uint16)
						childInvertedIndex = mpTrie.SearchInvertedIndexFromCacheOrDisk(childInvertIndexOffset, fileId, filePtr, invertedCache)
						if len(childInvertedIndex) > 0 {
							resMapFuzzy = UnionMapPos(resMapFuzzy, childInvertedIndex)
						}
					}
				}

			}

			if len(child.AddrCheck()) > 0 {
				if _, ok := child.AddrCheck()[fileId]; ok {
					if child.AddrCheck()[fileId].Addrlen() > 0 {
						childAddrOffset := child.AddrCheck()[fileId].AddrblkOffset()
						childAddrOffsets := mpTrie.SearchAddrOffsetsFromCacheOrDisk(childAddrOffset, fileId, filePtr, addrCache)
						if len(childAddrOffsets) > 0 {
							resMapFuzzy = FuzzySearchAddr(resMapFuzzy, childAddrOffsets, fileId, filePtr, invertedCache)
						}
					}
				}

			}

			resMapFuzzy = FuzzySearchChildrens(child, resMapFuzzy, fileId, filePtr, addrCache, invertedCache)
		}
	}
	return resMapFuzzy
}

func FuzzySearchAddr(resMapFuzzy *[]utils.SeriesId, addrOffsets map[uint64]uint16, fileId int, filePtr map[int]*os.File, invertedCache *mpTrie.InvertedCache) *[]utils.SeriesId {
	if addrOffsets == nil || len(addrOffsets) == 0 {
		return resMapFuzzy
	}
	for addr, _ := range addrOffsets {
		addrInvertedIndex := mpTrie.SearchInvertedIndexFromCacheOrDisk(addr, fileId, filePtr, invertedCache)

		resMapFuzzy = UnionMapPos(resMapFuzzy, addrInvertedIndex)
	}
	return resMapFuzzy
}

func TokenFuzzyReadInver(resMapFuzzy *[]utils.SeriesId, token string, indexRoot *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) *[]utils.SeriesId {
	tokenArr := []string{token}
	var invertIndexOffset uint64
	var addrOffset uint64
	var indexNode *mpTrie.SearchTreeNode
	var invertIndex1 utils.Inverted_index

	invertIndexOffset, addrOffset, indexNode = tokenMatchQuery.SearchNodeAddrFromPersistentIndexTree(fileId, tokenArr, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
	if len(indexNode.InvtdCheck()) > 0 {
		if _, ok := indexNode.InvtdCheck()[fileId]; ok {
			if indexNode.InvtdCheck()[fileId].Invtdlen() > 0 {
				invertIndex1 = mpTrie.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, fileId, filePtr, invertedCache)
			}
		}
	}
	resMapFuzzy = UnionMapPos(resMapFuzzy, invertIndex1)
	resMapFuzzy = FuzzySearchChildrens(indexNode, resMapFuzzy, fileId, filePtr, addrCache, invertedCache)
	if len(indexNode.AddrCheck()) > 0 {
		if _, ok := indexNode.AddrCheck()[fileId]; ok {
			if indexNode.AddrCheck()[fileId].Addrlen() > 0 {
				addrOffsets := mpTrie.SearchAddrOffsetsFromCacheOrDisk(addrOffset, fileId, filePtr, addrCache)
				if indexNode != nil && len(addrOffsets) > 0 {
					resMapFuzzy = FuzzySearchAddr(resMapFuzzy, addrOffsets, fileId, filePtr, invertedCache)
				}
			}
		}

	}
	return resMapFuzzy
}

func FuzzySearchComparedWithES(shortIndex map[int]map[string]struct{},longIndex map[string]map[int]map[utils.FuzzyPrefixGram]struct{},searchSingleToken string, indexRoot *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache, distance, prefixlen int,mapRes *[]utils.SeriesId) {
	q := prefixlen
	lensearchToken := len(searchSingleToken)
	if lensearchToken<q*distance+1+q-1{
		for key,value:=range shortIndex{
			if key > lensearchToken+distance || key < lensearchToken-distance {
				continue
			}else{
				for str:=range value{
					verifyresult := VerifyED(searchSingleToken, str, distance)
					if verifyresult {
						mapRes = TokenFuzzyReadInver(mapRes, str, indexRoot, fileId, filePtr, addrCache, invertedCache)
					}
				}
				continue
			}
		}

		indexInver:=make(map[string]struct{})
		for _,preIndex:=range longIndex{
			for lenData,indexData:=range preIndex {
				if lenData>lensearchToken+distance||lenData<lensearchToken-distance {
					continue
				}else {
					for data:=range indexData{
						indexInver[data.Gram()]= struct{}{}
					}
				}
			}

		}
		for key, _ := range indexInver {
			verifyresult3 := VerifyED(searchSingleToken, key, distance)
			if verifyresult3 {
				mapRes = TokenFuzzyReadInver(mapRes, key, indexRoot, fileId, filePtr, addrCache, invertedCache)
			}
		}
	}else{
		var qgramSearch = make([]utils.FuzzyPrefixGram, 0)
		for i := 0; i < lensearchToken-q+1; i++ {
			qgramSearch = append(qgramSearch, utils.NewFuzzyPrefixGram(searchSingleToken[i:i+q], int8(i)))
		}
		sort.SliceStable(qgramSearch, func(i, j int) bool {
			if qgramSearch[i].Gram() < qgramSearch[j].Gram() {
				return true
			}
			return false
		})
		prefixgramcount := q*distance + 1

		var mapsearchGram = make(map[string][]int8)
		if lensearchToken-q+1 >= prefixgramcount {
			for i := 0; i < prefixgramcount; i++ {
				mapsearchGram[qgramSearch[i].Gram()] = append(mapsearchGram[qgramSearch[i].Gram()], qgramSearch[i].Pos())
			}
		}

		for key,value:=range shortIndex{
			if key > lensearchToken+distance || key < lensearchToken-distance {
				continue
			}else{
				for str:=range value{
					verifyresult := VerifyED(searchSingleToken, str, distance)
					if verifyresult {
						mapRes = TokenFuzzyReadInver(mapRes, str, indexRoot, fileId, filePtr, addrCache, invertedCache)
					}
				}
				continue
			}
		}

		indexInver:=make(map[string]struct{})
		for key,value:=range mapsearchGram{
			for index,preIndex:=range longIndex[key]{
				if index>lensearchToken+distance||index<lensearchToken-distance{
					continue
				}else{
					for mapPre:=range preIndex{
						for n := 0; n < len(value); n++ {
							if AbsInt(value[n]-mapPre.Pos()) <= int8(distance) {
								indexInver[mapPre.Gram()]= struct{}{}
								break
							}
						}
					}

				}
			}
		}
		for key, _ := range indexInver {
			verifyresult2 := VerifyED(searchSingleToken, key, distance)
			if verifyresult2 {
				mapRes = TokenFuzzyReadInver(mapRes, key, indexRoot, fileId, filePtr, addrCache, invertedCache)
			}
		}
	}

}
func FuzzyTokenQueryTries(shortFuzzyIndex map[int]map[string]struct{},longFuzzyIndex  map[string]map[int]map[utils.FuzzyPrefixGram]struct{},searchStr string, indexRoots *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache, distance, prefixlen int) []utils.SeriesId {
	start := time.Now().UnixMicro()
	var resMap = make([]utils.SeriesId,0)
	for fileId, _ := range filePtr {
		FuzzySearchComparedWithES(shortFuzzyIndex,longFuzzyIndex,searchStr, indexRoots, fileId, filePtr, addrCache, invertedCache, distance, prefixlen,&resMap)
	}
	end := time.Now().UnixMicro()
	fmt.Println("allTime:")
	fmt.Println(float64(end-start) / 1000)
	fmt.Println("res count--------")
	fmt.Println(len(resMap))
	return resMap
}
