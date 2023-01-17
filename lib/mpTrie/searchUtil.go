package mpTrie

import (
	"github.com/openGemini/openGemini/lib/utils"
	"os"
)

//获取所有的 遍历一次
func SearchInvertedIndexFromCacheOrDisk(invertIndexOffset uint64, fileId int, filePtr map[int]*os.File, invertedCache *InvertedCache) map[utils.SeriesId][]uint16 {
	var invertedIndex map[utils.SeriesId][]uint16
	if invertedCache != nil && len(invertedCache.Blkcache()) > 0 {
		invertedIndex = invertedCache.Get(invertIndexOffset, fileId).Mpblk()
	}
	if len(invertedIndex) == 0 {
		invertedIndex = UnserializeInvertedListBlk(invertIndexOffset, filePtr[fileId]).Mpblk()
	}
	return invertedIndex
}

func SearchAddrOffsetsFromCacheOrDisk(addrOffset uint64, fileId int, filePtr map[int]*os.File, addrCache *AddrCache) map[uint64]uint16 {
	var addrOffsets map[uint64]uint16
	if addrCache != nil && len(addrCache.Blk()) > 0 {
		addrOffsets = addrCache.Get(addrOffset, fileId).Mpblk()
	}
	if len(addrOffsets) == 0 {
		addrOffsets = UnserializeAddrListBlk(addrOffset, filePtr[fileId]).Mpblk()
	}
	return addrOffsets
}

func MapToSlices(resArr utils.Inverted_index) []utils.SeriesId {
	res := make([]utils.SeriesId, 0)
	for key, _ := range resArr {
		res = append(res, key)
	}
	return res
}

func InvertdToMap(resArr utils.Inverted_index, resMap map[utils.SeriesId]struct{}) {
	for key, _ := range resArr {
		//res[key] = struct{}{}
		if _, ok := resMap[key]; !ok {
			resMap[key] = struct{}{}
		}
	}
}

func MapKeyToSlices(arrMap map[utils.SeriesId]struct{}) []utils.SeriesId {
	res := make([]utils.SeriesId, 0)
	for key, _ := range arrMap {
		res = append(res, key)
	}
	return res
}

func SearchInvertedListFromChildrensOfCurrentNode(indexNode *SearchTreeNode, invertIndex2 map[utils.SeriesId][]uint16, fileId int, filePtr map[int]*os.File, addrCache *AddrCache, invertedCache *InvertedCache) map[utils.SeriesId][]uint16 {
	if indexNode != nil {
		for _, child := range indexNode.Children() {
			//if len(child.InvtdCheck()) > 0 && child.InvtdCheck()[fileId].Invtdlen() > 0 {
			if len(child.InvtdCheck()) > 0 {
				if _, ok := child.InvtdCheck()[fileId]; ok {
					if child.InvtdCheck()[fileId].Invtdlen() > 0 {
						childInvertIndexOffset := child.InvtdCheck()[fileId].IvtdblkOffset()
						childInvertedIndex := make(map[utils.SeriesId][]uint16)
						childInvertedIndex = SearchInvertedIndexFromCacheOrDisk(childInvertIndexOffset, fileId, filePtr, invertedCache)
						if len(childInvertedIndex) > 0 {
							invertIndex2 = MergeMapsInvertLists(childInvertedIndex, invertIndex2)
						}
					}
				}
			}
			//if len(child.AddrCheck()) > 0 && child.AddrCheck()[fileId].Addrlen() > 0 {
			if len(child.AddrCheck()) > 0 {
				if _, ok := child.AddrCheck()[fileId]; ok {
					if child.AddrCheck()[fileId].Addrlen() > 0 {
						childAddrOffset := child.AddrCheck()[fileId].AddrblkOffset()
						childAddrOffsets := SearchAddrOffsetsFromCacheOrDisk(childAddrOffset, fileId, filePtr, addrCache)
						if len(childAddrOffsets) > 0 {
							var invertIndex3 = TurnAddr2InvertLists(childAddrOffsets, fileId, filePtr, invertedCache)
							invertIndex2 = MergeMapsTwoInvertLists(invertIndex3, invertIndex2)
						}
					}
				}
			}
			invertIndex2 = SearchInvertedListFromChildrensOfCurrentNode(child, invertIndex2, fileId, filePtr, addrCache, invertedCache)
		}
	}
	return invertIndex2
}

func SearchInvertedListFromChildrensOfCurrentNode2(indexNode *SearchTreeNode, arrMap map[utils.SeriesId]struct{}, fileId int, filePtr map[int]*os.File, addrCache *AddrCache, invertedCache *InvertedCache) map[utils.SeriesId]struct{} {
	if indexNode != nil {
		for _, child := range indexNode.Children() {
			if len(child.InvtdCheck()) > 0 {
				if _, ok := child.InvtdCheck()[fileId]; ok {
					if child.InvtdCheck()[fileId].Invtdlen() > 0 {
						childInvertIndexOffset := child.InvtdCheck()[fileId].IvtdblkOffset()
						childInvertedIndex := make(utils.Inverted_index)
						childInvertedIndex = SearchInvertedIndexFromCacheOrDisk(childInvertIndexOffset, fileId, filePtr, invertedCache)
						if len(childInvertedIndex) > 0 {
							arrMap = MergeMapsKeys(childInvertedIndex, arrMap)
						}
					}
				}
			}

			if len(child.AddrCheck()) > 0 {
				if _, ok := child.AddrCheck()[fileId]; ok {
					if child.AddrCheck()[fileId].Addrlen() > 0 {
						childAddrOffset := child.AddrCheck()[fileId].AddrblkOffset()
						childAddrOffsets := SearchAddrOffsetsFromCacheOrDisk(childAddrOffset, fileId, filePtr, addrCache)
						if len(childAddrOffsets) > 0 {
							arrMap = TurnAddr2InvertLists2(childAddrOffsets, fileId, filePtr, invertedCache, arrMap)
						}
					}
				}
			}
			arrMap = SearchInvertedListFromChildrensOfCurrentNode2(child, arrMap, fileId, filePtr, addrCache, invertedCache)
		}
	}
	return arrMap
}

func TurnAddr2InvertLists(addrOffsets map[uint64]uint16, fileId int, filePtr map[int]*os.File, invertedCache *InvertedCache) map[utils.SeriesId][]uint16 {
	var res map[utils.SeriesId][]uint16
	if addrOffsets == nil || len(addrOffsets) == 0 {
		return res
	}
	for addr, offset := range addrOffsets {
		invertIndex3 := make(map[utils.SeriesId][]uint16)
		addrInvertedIndex := SearchInvertedIndexFromCacheOrDisk(addr, fileId, filePtr, invertedCache)
		for key, value := range addrInvertedIndex {
			list := make([]uint16, 0)
			for i := 0; i < len(value); i++ {
				list = append(list, value[i]+offset)
			}
			invertIndex3[key] = list
		}
		res = MergeMapsTwoInvertLists(invertIndex3, res)
	}
	return res
}

func TurnAddr2InvertLists2(addrOffsets map[uint64]uint16, fileId int, filePtr map[int]*os.File, invertedCache *InvertedCache, arrMap map[utils.SeriesId]struct{}) map[utils.SeriesId]struct{} {
	if addrOffsets == nil || len(addrOffsets) == 0 {
		return arrMap
	}
	for addr, _ := range addrOffsets {
		addrInvertedIndex := SearchInvertedIndexFromCacheOrDisk(addr, fileId, filePtr, invertedCache)
		arrMap = MergeMapsKeys(addrInvertedIndex, arrMap)
	}
	return arrMap
}

func MergeMapsKeys(index utils.Inverted_index, arrMap map[utils.SeriesId]struct{}) map[utils.SeriesId]struct{} {
	for key, _ := range index {
		if _, ok := arrMap[key]; !ok {
			arrMap[key] = struct{}{}
		}
	}
	return arrMap
}

func MergeMapsInvertLists(map1 map[utils.SeriesId][]uint16, map2 map[utils.SeriesId][]uint16) map[utils.SeriesId][]uint16 {
	if len(map2) > 0 {
		for sid1, list1 := range map1 {
			if list2, ok := map2[sid1]; !ok {
				map2[sid1] = list1
			} else {
				list2 = append(list2, list1...)
				list2 = UniqueArr(list2)
				//sort.Slice(list2, func(i, j int) bool { return list2[i] < list2[j] })
				map2[sid1] = list2
			}
		}
	} else {
		map2 = DeepCopy(map1)
	}
	return map2
}

func UniqueArr(m []uint16) []uint16 {
	d := make([]uint16, 0)
	tempMap := make(map[uint16]bool, len(m))
	for _, v := range m {
		if tempMap[v] == false {
			tempMap[v] = true
			d = append(d, v)
		}
	}
	return d
}

func DeepCopy(src map[utils.SeriesId][]uint16) map[utils.SeriesId][]uint16 {
	dst := make(map[utils.SeriesId][]uint16)
	for key, value := range src {
		list := make([]uint16, 0)
		for i := 0; i < len(value); i++ {
			list = append(list, value[i])
		}
		dst[key] = list
	}
	return dst
}

func MergeMapsTwoInvertLists(map1 map[utils.SeriesId][]uint16, map2 map[utils.SeriesId][]uint16) map[utils.SeriesId][]uint16 {
	if len(map1) == 0 {
		return map2
	} else if len(map2) == 0 {
		return map1
	} else if len(map1) < len(map2) {
		for sid1, list1 := range map1 {
			if list2, ok := map2[sid1]; !ok {
				map2[sid1] = list1
			} else {
				list2 = append(list2, list1...)
				list2 = UniqueArr(list2)
				//sort.Slice(list2, func(i, j int) bool { return list2[i] < list2[j] })
				map2[sid1] = list2
			}
		}
		return map2
	} else {
		for sid1, list1 := range map2 {
			if list2, ok := map1[sid1]; !ok {
				map1[sid1] = list1
			} else {
				list2 = append(list2, list1...)
				list2 = UniqueArr(list2)
				//sort.Slice(list2, func(i, j int) bool { return list2[i] < list2[j] })
				map1[sid1] = list2
			}
		}
		return map1
	}
}

func MergeMapsThreeInvertLists(invertedindexes []utils.Inverted_index) utils.Inverted_index {
	res := make(map[utils.SeriesId][]uint16)
	res = MergeMapsInvertLists(invertedindexes[0], res)
	res = MergeMapsInvertLists(invertedindexes[1], res)
	res = MergeMapsInvertLists(invertedindexes[2], res)
	return res
}
