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

func UnionMapPos(map1 map[utils.SeriesId]struct{}, mapPos map[utils.SeriesId][]uint16) map[utils.SeriesId]struct{} {
	if len(mapPos) == 0 {
		return map1
	}
	for key, _ := range mapPos {
		if _, ok := map1[key]; !ok {
			map1[key] = struct{}{}

		}
	}
	return map1
}
func FuzzySearchChildrens(indexNode *mpTrie.SearchTreeNode, resMapFuzzy map[utils.SeriesId]struct{}, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
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

func FuzzySearchAddr(resMapFuzzy map[utils.SeriesId]struct{}, addrOffsets map[uint64]uint16, fileId int, filePtr map[int]*os.File, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {

	if addrOffsets == nil || len(addrOffsets) == 0 {
		return resMapFuzzy
	}
	for addr, _ := range addrOffsets {
		addrInvertedIndex := mpTrie.SearchInvertedIndexFromCacheOrDisk(addr, fileId, filePtr, invertedCache)

		resMapFuzzy = UnionMapPos(resMapFuzzy, addrInvertedIndex)
	}
	return resMapFuzzy
}
func TokenFuzzyReadInver(resMapFuzzy map[utils.SeriesId]struct{}, token string, indexRoot *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
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

func FuzzySearchComparedWithES(searchSingleToken string, indexRoot *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache, distance, prefixlen int) map[utils.SeriesId]struct{} {
	sum := 0
	sumPass := 0
	mapRes := make(map[utils.SeriesId]struct{})
	q := prefixlen
	lensearchToken := len(searchSingleToken)
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
	for i, _ := range indexRoot.Children() {
		lenChildrendata := len(indexRoot.Children()[i].Data())
		if lenChildrendata > lensearchToken+distance || lenChildrendata < lensearchToken-distance {
			continue
		} else if lenChildrendata-q+1 < prefixgramcount || lensearchToken-q+1 < prefixgramcount {
			sum++
			verifyresult := VerifyED(searchSingleToken, indexRoot.Children()[i].Data(), distance)
			if verifyresult {
				sumPass++
				mapRes = TokenFuzzyReadInver(mapRes, indexRoot.Children()[i].Data(), indexRoot, fileId, filePtr, addrCache, invertedCache)
			}
			continue
		} else {
			flagCommon := 0
			for k := 0; k < prefixgramcount; k++ {
				_, ok := mapsearchGram[indexRoot.Children()[i].PrefixGrams()[k].Gram()]
				if ok {
					for n := 0; n < len(mapsearchGram[indexRoot.Children()[i].PrefixGrams()[k].Gram()]); n++ {
						if AbsInt(mapsearchGram[indexRoot.Children()[i].PrefixGrams()[k].Gram()][n]-indexRoot.Children()[i].PrefixGrams()[k].Pos()) <= int8(distance) {
							flagCommon = 1
							sum++
							verifyresult2 := VerifyED(searchSingleToken, indexRoot.Children()[i].Data(), distance)

							if verifyresult2 {
								sumPass++
								mapRes = TokenFuzzyReadInver(mapRes, indexRoot.Children()[i].Data(), indexRoot, fileId, filePtr, addrCache, invertedCache)
							}
							break
						}
					}
					if flagCommon == 1 {
						break
					}
				}
			}
			continue
		}
	}
	return mapRes
}

func FuzzyTokenQueryTries(searchStr string, indexRoots *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache, distance, prefixlen int) map[utils.SeriesId]struct{} {
	start := time.Now().UnixMicro()
	var resArr = make(map[utils.SeriesId]struct{})
	for fileId, _ := range filePtr {
		resArr = utils.Or(FuzzySearchComparedWithES(searchStr, indexRoots, fileId, filePtr, addrCache, invertedCache, distance, prefixlen), resArr)
	}
	end := time.Now().UnixMicro()
	fmt.Println(float64(end-start) / 1000)
	return resArr
}
