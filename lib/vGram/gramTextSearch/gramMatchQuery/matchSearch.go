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
package gramMatchQuery

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"os"
	"sort"
	"time"
)

func MatchSearch(searchStr string, root *gramClvc.TrieTreeNode, indexRoots *mpTrie.SearchTreeNode, qmin int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	start := time.Now().UnixMicro()
	var vgMap = make(map[uint16]string)
	gramIndex.VGConsBasicIndex(root, qmin, searchStr, vgMap)
	var resArr = make(map[utils.SeriesId]struct{})
	for fileId, _ := range filePtr {
		MatchSearch2(vgMap, indexRoots, fileId, filePtr, addrCache, invertedCache, resArr)
	}
	end := time.Now().UnixMicro()
	fmt.Println("精确查询时间:(ms)", float64(end-start)/1000)
	fmt.Println("精确结果条数:", len(resArr))
	return resArr
}

func MatchSearch2(vgMap map[uint16]string, indexRoot *mpTrie.SearchTreeNode, fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache, resMap map[utils.SeriesId]struct{}) {
	if len(vgMap) == 1 {
		gram := vgMap[0]
		var invertIndexOffset uint64
		var addrOffset uint64
		var indexNode *mpTrie.SearchTreeNode
		var invertIndex1 utils.Inverted_index
		invertIndexOffset, addrOffset, indexNode = SearchNodeAddrFromPersistentIndexTree(fileId, gram, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
		if len(indexNode.InvtdCheck()) > 0 {
			if _, ok := indexNode.InvtdCheck()[fileId]; ok {
				if indexNode.InvtdCheck()[fileId].Invtdlen() > 0 {
					invertIndex1 = mpTrie.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, fileId, filePtr, invertedCache)
				}
			}
		}
		resMap = mpTrie.MergeMapsKeys(invertIndex1, resMap)
		resMap = mpTrie.SearchInvertedListFromChildrensOfCurrentNode2(indexNode, resMap, fileId, filePtr, addrCache, invertedCache)
		if len(indexNode.AddrCheck()) > 0 {
			if _, ok := indexNode.AddrCheck()[fileId]; ok {
				if indexNode.AddrCheck()[fileId].Addrlen() > 0 {
					addrOffsets := mpTrie.SearchAddrOffsetsFromCacheOrDisk(addrOffset, fileId, filePtr, addrCache)
					if indexNode != nil && len(addrOffsets) > 0 {
						resMap = mpTrie.TurnAddr2InvertLists2(addrOffsets, fileId, filePtr, invertedCache, resMap)
					}
				}
			}
		}
	} else {
		var sortSumInvertList = make([]SortKey, 0)
		for x := range vgMap {
			gram := vgMap[x]
			if gram != "" {
				freq := SearchInvertedListLengthFromGram(gram, indexRoot, 0, 1)
				sortSumInvertList = append(sortSumInvertList, NewSortKey(x, freq, gram))
			}
		}
		sort.SliceStable(sortSumInvertList, func(i, j int) bool {
			if sortSumInvertList[i].sizeOfInvertedList < sortSumInvertList[j].sizeOfInvertedList {
				return true
			}
			return false
		})
		//end21 := time.Now().UnixMicro()
		//var sum1 uint64 = 0
		//var sum2 uint64 = 0
		//var sum3 uint64 = 0
		//var sum4 uint64 = 0
		var resArr = make(utils.Inverted_index, 0)
		var prePos uint16 = 0
		var nowPos uint16 = 0
		for m := 0; m < len(sortSumInvertList); m++ {
			gramArr := sortSumInvertList[m].gram
			if gramArr != "" {
				invertIndexes := make([]utils.Inverted_index, 3)
				var invertIndexOffset uint64
				var addrOffset uint64
				var indexNode *mpTrie.SearchTreeNode
				var invertIndex1 utils.Inverted_index
				var invertIndex2 utils.Inverted_index
				var invertIndex3 utils.Inverted_index
				//start21 := time.Now().UnixMicro()
				invertIndexOffset, addrOffset, indexNode = SearchNodeAddrFromPersistentIndexTree(fileId, gramArr, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
				if len(indexNode.InvtdCheck()) > 0 {
					if _, ok := indexNode.InvtdCheck()[fileId]; ok {
						if indexNode.InvtdCheck()[fileId].Invtdlen() > 0 {
							invertIndex1 = mpTrie.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, fileId, filePtr, invertedCache)
						}
					}
				}
				//end22 := time.Now().UnixMicro()
				//sum1 += uint64(end22 - start21)
				//start22 := time.Now().UnixMicro()
				invertIndex2 = mpTrie.SearchInvertedListFromChildrensOfCurrentNode(indexNode, invertIndex2, fileId, filePtr, addrCache, invertedCache)
				//end23 := time.Now().UnixMicro()
				//sum2 += uint64(end23 - start22)
				//start23 := time.Now().UnixMicro()
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
				//end24 := time.Now().UnixMicro()
				//sum3 += uint64(end24 - start23)
				//start24 := time.Now().UnixMicro()
				invertIndexes[0] = invertIndex1
				invertIndexes[1] = invertIndex2
				invertIndexes[2] = invertIndex3
				if invertIndexes == nil || (len(invertIndexes[0]) == 0 && len(invertIndexes[1]) == 0 && len(invertIndexes[2]) == 0) {
					break
				}
				if m == 0 {
					resArr = mpTrie.MergeMapsThreeInvertLists(invertIndexes)
					prePos = sortSumInvertList[m].offset
				} else {
					nowPos = sortSumInvertList[m].offset
					queryDis := nowPos - prePos
					for sid, posList1 := range resArr {
						findFlag := false
						for i := 0; i < 3; i++ {
							if _, ok := invertIndexes[i][sid]; ok {
								posList2 := invertIndexes[i][sid]
								for z1 := 0; z1 < len(posList1); z1++ {
									z1Pos := posList1[z1]
									for z2 := 0; z2 < len(posList2); z2++ {
										z2Pos := posList2[z2]
										if queryDis == z2Pos-z1Pos {
											findFlag = true
											break
										}
									}
									if findFlag == true {
										break
									}
								}
							}
							if findFlag == true {
								break
							}
						}
						if findFlag == false {
							delete(resArr, sid)
						}
					}
				}
				//end25 := time.Now().UnixMicro()
				//sum4 += uint64(end25 - start24)
			}
		}
		//fmt.Println("sort and freq:", (end21-start1)/1000)
		//fmt.Println("sum1:", sum1/1000)
		//fmt.Println("sum2:", sum2/1000)
		//fmt.Println("sum3:", sum3/1000)
		//fmt.Println("sum4:", sum4/1000)
		mpTrie.InvertdToMap(resArr, resMap)
	}
}

func SearchNodeAddrFromPersistentIndexTree(fileId int, gramArr string, indexRoot *mpTrie.SearchTreeNode, i int, invertIndexOffset uint64, addrOffset uint64, indexNode *mpTrie.SearchTreeNode) (uint64, uint64, *mpTrie.SearchTreeNode) {
	if indexRoot == nil {
		return invertIndexOffset, addrOffset, indexNode
	}
	if i < len(gramArr)-1 && indexRoot.Children()[int(gramArr[i])] != nil {
		invertIndexOffset, addrOffset, indexNode = SearchNodeAddrFromPersistentIndexTree(fileId, gramArr, indexRoot.Children()[int(gramArr[i])], i+1, invertIndexOffset, addrOffset, indexNode)
	}
	if i == len(gramArr)-1 && indexRoot.Children()[int(gramArr[i])] != nil {
		if len(indexRoot.Children()[int(gramArr[i])].InvtdCheck()) > 0 {
			if _, ok := indexRoot.Children()[int(gramArr[i])].InvtdCheck()[fileId]; ok {
				if indexRoot.Children()[int(gramArr[i])].InvtdCheck()[fileId].Invtdlen() > 0 {
					invertIndexOffset = indexRoot.Children()[int(gramArr[i])].InvtdCheck()[fileId].IvtdblkOffset()
				}
			}
		}
		if len(indexRoot.Children()[int(gramArr[i])].AddrCheck()) > 0 {
			if _, ok := indexRoot.Children()[int(gramArr[i])].AddrCheck()[fileId]; ok {
				if indexRoot.Children()[int(gramArr[i])].AddrCheck()[fileId].Addrlen() > 0 {
					addrOffset = indexRoot.Children()[int(gramArr[i])].AddrCheck()[fileId].AddrblkOffset()
				}
			}
		}
		indexNode = indexRoot.Children()[int(gramArr[i])]
	}
	return invertIndexOffset, addrOffset, indexNode
}

func SearchInvertedListLengthFromGram(gramArr string, indexRoot *mpTrie.SearchTreeNode, i int, freq int) int {
	if indexRoot == nil {
		return freq
	}
	if i < len(gramArr)-1 && indexRoot.Children()[int(gramArr[i])] != nil {
		freq = SearchInvertedListLengthFromGram(gramArr, indexRoot.Children()[int(gramArr[i])], i+1, freq)
	}
	if i == len(gramArr)-1 && indexRoot.Children()[int(gramArr[i])] != nil {
		freq = indexRoot.Children()[int(gramArr[i])].Freq()
	}
	return freq
}
