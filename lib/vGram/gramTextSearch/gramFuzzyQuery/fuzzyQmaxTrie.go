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
	"os"
	"strings"
	"time"
)

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

func QmaxTrieListPath(stackNode []*gramIndex.LogTreeNode,countPop *int,root *gramIndex.LogTreeNode, path string, collection map[string]struct{}, query string, distance int, qmin int) {
	if len(root.Children()) == 0 {
		path = path + root.Data()
		//fmt.Println(path)
		minFuzzyStr,posLast:= MinimumFuzzySubstring(query, distance, path, qmin)
		if minFuzzyStr == "" {
			*countPop=0
			return
		}
		JoinCollection(minFuzzyStr, collection)
		*countPop=len(path)-posLast-1
		return
	} else {
		path = path + root.Data()
		stackNode= append(stackNode, root)
		for _, child := range root.Children() {
			QmaxTrieListPath(stackNode,countPop,child, path, collection, query, distance, qmin)
			if stackNode[len(stackNode)-1]==root && *countPop>0{
				stackNode=stackNode[:len(stackNode)-1]
				*countPop--
				break
			}
			//如果是栈顶且弹栈数>0，就弹栈,break
		}
		if len(stackNode)>1{
			stackNode=stackNode[:len(stackNode)-1]
		}

	}
}

func MinimumFuzzySubstring(query string, distance int, path string, qmin int) (string,int) {
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
		return "",0
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
			return path[column : column+qmin],column+qmin-1
		} else {
			return path[len(path)-qmin:],len(path)-1
		}
	}
	return path[column:col],col-1

}

func JoinCollection(str string, collection map[string]struct{}) {
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
				collection[str] = struct{}{}
				flag = true
			}
		}
	}
	if flag == false {
		collection[str] = struct{}{}
	}
}

func UnionMapGramFuzzy(map1 map[utils.SeriesId]struct{}, map2 map[utils.SeriesId]struct{}) map[utils.SeriesId]struct{} {
	if len(map2) == 0 {
		return map1
	}
	for key, _ := range map2 {
		if _, ok := map1[key]; !ok {
			map1[key] = struct{}{}
		}
	}
	return map1
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
func FuzzyReadInver(resMapFuzzy map[utils.SeriesId]struct{}, vgMap map[uint16]string, indexRoot *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	gram := ""
	for _, value := range vgMap {
		gram = value
	}

	var invertIndexOffset uint64
	var addrOffset uint64
	var indexNode *mpTrie.SearchTreeNode
	var invertIndex1 utils.Inverted_index

	invertIndexOffset, addrOffset, indexNode = gramMatchQuery.SearchNodeAddrFromPersistentIndexTree(fileId, gram, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
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
func FuzzyQueryGramQmaxTrie(rootFuzzyTrie *gramIndex.LogTreeNode, searchStr string, dicRootNode *gramClvc.TrieTreeNode,
	indexRoots *mpTrie.SearchTreeNode, qmin int, qmax int, distance int, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {

	resMapFuzzy := make(map[utils.SeriesId]struct{})
	if len(searchStr) > qmax+distance {
		fmt.Println("error:查询语句长度大于qmax+distance,无法匹配结果")
		return resMapFuzzy
	}
	collectionMinStr := make(map[string]struct{})
	stackNode:=make([]*gramIndex.LogTreeNode,0)
	countPop:=0
	QmaxTrieListPath(stackNode,&countPop,rootFuzzyTrie, "", collectionMinStr, searchStr, distance, qmin)
	if len(collectionMinStr) == 1 {
		for key, _ := range collectionMinStr { //(key, dicRootNode, indexRootNode, qmin)
			var vgMap = make(map[uint16]string)
			gramIndex.VGConsBasicIndex(dicRootNode, qmin, key, vgMap)
			if len(vgMap) == 1 {
				resMapFuzzy = FuzzyReadInver(resMapFuzzy, vgMap, indexRoots, fileId, filePtr, addrCache, invertedCache)
				return resMapFuzzy
			} else {
				arrayNew := make(map[utils.SeriesId]struct{})//gramMatchQuery.MatchSearch2(vgMap, indexRoots, fileId, filePtr, addrCache, invertedCache, nil) todo
				return arrayNew
			}
		}
	}
	for key, _ := range collectionMinStr { //(key, dicRootNode, indexRootNode, qmin)
		var vgMap = make(map[uint16]string)
		gramIndex.VGConsBasicIndex(dicRootNode, qmin, key, vgMap)
		if len(vgMap) == 1 {
			resMapFuzzy = FuzzyReadInver(resMapFuzzy, vgMap, indexRoots, fileId, filePtr, addrCache, invertedCache)
		} else {
			arrayNew := make(map[utils.SeriesId]struct{})//gramMatchQuery.MatchSearch2(vgMap, indexRoots, fileId, filePtr, addrCache, invertedCache, nil) todo
			resMapFuzzy = UnionMapGramFuzzy(resMapFuzzy, arrayNew)
		}
	}
	return resMapFuzzy
}

func FuzzyQueryGramQmaxTries(rootFuzzyTrie *gramIndex.LogTreeNode, searchStr string, root *gramClvc.TrieTreeNode, indexRoots *mpTrie.SearchTreeNode, qmin int, qmax int, distance int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	start := time.Now().UnixMicro()
	var resArr = make(map[utils.SeriesId]struct{})
	for fileId, _ := range filePtr {
		resArr = utils.Or(FuzzyQueryGramQmaxTrie(rootFuzzyTrie, searchStr, root, indexRoots, qmin, qmax, distance, fileId, filePtr, addrCache, invertedCache), resArr)
	}
	end := time.Now().UnixMicro()
	fmt.Println( float64(end-start)/1000)
	return resArr
}
