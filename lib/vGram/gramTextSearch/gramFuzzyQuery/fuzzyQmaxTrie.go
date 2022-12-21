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
package gramFuzzyQuery

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramMatchQuery"
	"strings"
)

type FuzzyEmpty struct{}

var fuzzyEmpty FuzzyEmpty

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

func QmaxTrieListPath(root *gramIndex.LogTreeNode, path string, collection map[string]FuzzyEmpty, query string, distance int, qmin int) {
	if len(root.Children()) == 0 {
		path = path + root.Data()
		//fmt.Println(path)
		if len(query) <= len(path)+distance {
			minFuzzyStr := MinimumFuzzySubstring(query, distance, path, qmin)
			if minFuzzyStr == "" {
				return
			}
			JoinCollection(minFuzzyStr, collection)
		} else if len(query) > len(path)+distance {
			return
		}
		return
	} else {
		path = path + root.Data()
		for _, child := range root.Children() {
			QmaxTrieListPath(child, path, collection, query, distance, qmin)
		}
	}
}

func MinimumFuzzySubstring(query string, distance int, path string, qmin int) string {
	l1 := len(query)
	l2 := len(path)

	dp := make([][]int, l1+1)

	dp[0] = make([]int, l2+1)
	for j := 0; j <= l2; j++ {
		dp[0][j] = 0
	}

	for i := 1; i <= l1; i++ {
		dp[i] = make([]int, l2+1)
		dp[i][0] = i
		for j := 1; j <= l2; j++ {
			if query[i-1:i] == path[j-1:j] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = MinThree(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]) + 1
			}
		}
	}
	col := 0
	minNum := dp[l1][0]
	for k := 1; k <= l2; k++ {
		if dp[l1][k] < minNum {
			minNum = dp[l1][k]
			col = k
		}
	}
	if dp[l1][col] > distance {
		return ""
	}
	//回溯
	var row int
	var column int
	for row, column = l1, col; row > 0; {
		if row != 0 && dp[row-1][column]+1 == dp[row][column] {
			row--
			continue
		} else if column != 0 && row != 0 && dp[row-1][column-1]+1 == dp[row][column] {
			row--
			column--
			continue
		} else if column != 0 && dp[row][column-1]+1 == dp[row][column] {
			column--
			continue
		}
		row--
		column--
	}
	if len(path[column:col]) < qmin {
		if column <= len(path)-qmin {
			return path[column : column+qmin]
		} else {
			return path[len(path)-qmin:]
		}
	}
	return path[column:col]

}

func JoinCollection(str string, collection map[string]FuzzyEmpty) {
	_, okh := collection[str]
	flag := false
	if okh {
		return
	} else {
		for key, _ := range collection {
			if strings.Contains(str, key) {
				flag = true
				return
			}
			if strings.Contains(key, str) {
				delete(collection, key)
				collection[str] = fuzzyEmpty
				flag = true
			}
		}
	}
	if flag == false {
		collection[str] = fuzzyEmpty
	}
}

func UnionArrayMapGramFuzzy(resArrFuzzy *[]utils.SeriesId, map1 map[utils.SeriesId]FuzzyEmpty, array []utils.SeriesId) (map[utils.SeriesId]FuzzyEmpty, *[]utils.SeriesId) {
	if len(array) == 0 {
		return map1, resArrFuzzy
	}
	for i := 0; i < len(array); i++ {
		if _, ok := map1[array[i]]; !ok {
			map1[array[i]] = fuzzyEmpty
			*resArrFuzzy = append(*resArrFuzzy, array[i])
		}
	}
	return map1, resArrFuzzy
}
func UnionMapPos(resArrFuzzy *[]utils.SeriesId, map1 map[utils.SeriesId]FuzzyEmpty, mapPos map[utils.SeriesId][]uint16) (map[utils.SeriesId]FuzzyEmpty, *[]utils.SeriesId) {
	if len(mapPos) == 0 {
		return map1, resArrFuzzy
	}
	for key, _ := range mapPos {
		if _, ok := map1[key]; !ok {
			map1[key] = fuzzyEmpty
			*resArrFuzzy = append(*resArrFuzzy, key)
		}
	}
	return map1, resArrFuzzy
}
func FuzzySearchChildrens(indexNode *mpTrie.SearchTreeNode, resArrFuzzy *[]utils.SeriesId, resMapFuzzy map[utils.SeriesId]FuzzyEmpty, buffer []byte, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) (map[utils.SeriesId]FuzzyEmpty, *[]utils.SeriesId) {
	if indexNode != nil {
		for _, child := range indexNode.Children() {
			if child.Invtdlen() > 0 {
				childInvertIndexOffset := child.InvtdInfo().IvtdblkOffset()
				childInvertedIndex := make(map[utils.SeriesId][]uint16)
				childInvertedIndex = mpTrie.SearchInvertedIndexFromCacheOrDisk(childInvertIndexOffset, buffer, invertedCache)
				if len(childInvertedIndex) > 0 {
					resMapFuzzy, resArrFuzzy = UnionMapPos(resArrFuzzy, resMapFuzzy, childInvertedIndex)
				}
			}

			if child.Addrlen() > 0 {
				childAddrOffset := child.AddrInfo().AddrblkOffset()
				childAddrOffsets := mpTrie.SearchAddrOffsetsFromCacheOrDisk(childAddrOffset, buffer, addrCache)
				if len(childAddrOffsets) > 0 {
					resMapFuzzy, resArrFuzzy = FuzzySearchAddr(resArrFuzzy, resMapFuzzy, childAddrOffsets, buffer, invertedCache)
				}
			}
			resMapFuzzy, resArrFuzzy = FuzzySearchChildrens(child, resArrFuzzy, resMapFuzzy, buffer, addrCache, invertedCache)
		}
	}
	return resMapFuzzy, resArrFuzzy
}

func FuzzySearchAddr(resArrFuzzy *[]utils.SeriesId, resMapFuzzy map[utils.SeriesId]FuzzyEmpty, addrOffsets map[uint64]uint16, buffer []byte, invertedCache *mpTrie.InvertedCache) (map[utils.SeriesId]FuzzyEmpty, *[]utils.SeriesId) {

	if addrOffsets == nil || len(addrOffsets) == 0 {
		return resMapFuzzy, resArrFuzzy
	}
	for addr, _ := range addrOffsets {
		addrInvertedIndex := mpTrie.SearchInvertedIndexFromCacheOrDisk(addr, buffer, invertedCache)

		resMapFuzzy, resArrFuzzy = UnionMapPos(resArrFuzzy, resMapFuzzy, addrInvertedIndex)
	}
	return resMapFuzzy, resArrFuzzy
}
func FuzzyReadInver(resArrFuzzy *[]utils.SeriesId, resMapFuzzy map[utils.SeriesId]FuzzyEmpty, vgMap map[uint16]string, indexRoot *mpTrie.SearchTreeNode, buffer []byte, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) (map[utils.SeriesId]FuzzyEmpty, *[]utils.SeriesId) {
	gram := ""
	for _, value := range vgMap {
		gram = value
	}

	var invertIndexOffset uint64
	var addrOffset uint64
	var indexNode *mpTrie.SearchTreeNode
	var invertIndex1 utils.Inverted_index

	invertIndexOffset, addrOffset, indexNode = gramMatchQuery.SearchNodeAddrFromPersistentIndexTree(gram, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
	if indexNode.Invtdlen() > 0 {
		invertIndex1 = mpTrie.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, buffer, invertedCache)
	}
	resMapFuzzy, resArrFuzzy = UnionMapPos(resArrFuzzy, resMapFuzzy, invertIndex1)
	resMapFuzzy, resArrFuzzy = FuzzySearchChildrens(indexNode, resArrFuzzy, resMapFuzzy, buffer, addrCache, invertedCache)
	if indexNode.Addrlen() > 0 {
		addrOffsets := mpTrie.SearchAddrOffsetsFromCacheOrDisk(addrOffset, buffer, addrCache)
		if indexNode != nil && len(addrOffsets) > 0 {
			resMapFuzzy, resArrFuzzy = FuzzySearchAddr(resArrFuzzy, resMapFuzzy, addrOffsets, buffer, invertedCache)
		}
	}
	return resMapFuzzy, resArrFuzzy

}
func FuzzyQueryGramQmaxTrie(rootFuzzyTrie *gramIndex.LogTreeNode, searchStr string, dicRootNode *gramClvc.TrieTreeNode,
	indexRoots *mpTrie.SearchTreeNode, qmin int, qmax int, distance int, buffer []byte, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) []utils.SeriesId {
	resArrFuzzy := make([]utils.SeriesId, 0)
	resPointerFuzzy := &resArrFuzzy
	resMapFuzzy := make(map[utils.SeriesId]FuzzyEmpty)
	if len(searchStr) > qmax+distance {
		fmt.Println("error:查询语句长度大于qmax+distance,无法匹配结果")
		return resArrFuzzy
	}
	collectionMinStr := make(map[string]FuzzyEmpty)
	QmaxTrieListPath(rootFuzzyTrie, "", collectionMinStr, searchStr, distance, qmin)
	if len(collectionMinStr) == 1 {
		for key, _ := range collectionMinStr { //(key, dicRootNode, indexRootNode, qmin)
			var vgMap = make(map[uint16]string)
			gramIndex.VGConsBasicIndex(dicRootNode, qmin, key, vgMap)
			if len(vgMap) == 1 {
				_, resPointerFuzzy = FuzzyReadInver(resPointerFuzzy, resMapFuzzy, vgMap, indexRoots, buffer, addrCache, invertedCache)
				return *resPointerFuzzy
			} else {
				arrayNew := gramMatchQuery.MatchSearch2(vgMap, indexRoots, buffer, addrCache, invertedCache)
				return arrayNew
			}
		}
	}
	for key, _ := range collectionMinStr { //(key, dicRootNode, indexRootNode, qmin)
		var vgMap = make(map[uint16]string)
		gramIndex.VGConsBasicIndex(dicRootNode, qmin, key, vgMap)
		if len(vgMap) == 1 {
			resMapFuzzy, resPointerFuzzy = FuzzyReadInver(resPointerFuzzy, resMapFuzzy, vgMap, indexRoots, buffer, addrCache, invertedCache)
		} else {
			arrayNew := gramMatchQuery.MatchSearch2(vgMap, indexRoots, buffer, addrCache, invertedCache)
			resMapFuzzy, resPointerFuzzy = UnionArrayMapGramFuzzy(resPointerFuzzy, resMapFuzzy, arrayNew)
		}
	}
	return *resPointerFuzzy
}

/*
func ArrayRemoveDuplicate(array []utils.SeriesId) []utils.SeriesId {
	result := make([]utils.SeriesId, 0)
	var sid uint64
	var time int64 = -1
	for i := 0; i < len(array); i++ {
		if sid == array[i].Id && time == array[i].Time {
			continue
		} else {
			sid = array[i].Id
			time = array[i].Time
			result = append(result, array[i])
		}
	}
	return result
}
func ArraySortAndRemoveDuplicate(array []utils.SeriesId) []utils.SeriesId {
	sort.SliceStable(array, func(i, j int) bool {
		if array[i].Id <= array[j].Id && array[i].Time <= array[j].Time {
			return true
		}
		return false
	})
	array = ArrayRemoveDuplicate(array)
	return array
}
func FuzzyQueryGramQmaxTrie(rootFuzzyTrie *gramIndex.LogTreeNode, searchStr string, dicRootNode *gramClvc.TrieTreeNode,
	indexRoots *mpTrie.SearchTreeNode, qmin int, qmax int, distance int, buffer []byte, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) []utils.SeriesId {
	resArrFuzzy := make([]utils.SeriesId, 0)
	if len(searchStr) > qmax+distance {
		fmt.Println("error:查询语句长度大于qmax+distance,无法匹配结果")
		return resArrFuzzy
	}
	collectionMinStr := make(map[string]FuzzyEmpty)
	QmaxTrieListPath(rootFuzzyTrie, "", collectionMinStr, searchStr, distance, qmin)
	for key, _ := range collectionMinStr { //(key, dicRootNode, indexRootNode, qmin)
		arrayNew := gramMatchQuery.MatchSearch(key, dicRootNode, indexRoots, qmin, buffer, addrCache, invertedCache)
		resArrFuzzy = append(resArrFuzzy, arrayNew...)
	}
	resArrFuzzy = ArraySortAndRemoveDuplicate(resArrFuzzy)
	return resArrFuzzy
}*/

/*func FuzzyQueryGramQmaxTries(rootFuzzyTrie *gramIndex.LogTreeNode, searchStr string, dicRootNode *gramClvc.TrieTreeNode,
	indexRoots []*decode.SearchTreeNode, qmin int, qmax int, distance int,
	buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) []utils.SeriesId {
	var resArr = make([]utils.SeriesId, 0)
	for i := 0; i < len(indexRoots); i++ {
		resArr = append(resArr, FuzzyQueryGramQmaxTrie(rootFuzzyTrie, searchStr, dicRootNode,
			indexRoots, qmin, qmax, distance, buffer, addrCache, invertedCache)...)
	}
	return resArr
}*/
