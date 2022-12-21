package mpTrie

import (
	"github.com/openGemini/openGemini/lib/utils"
)

func SearchInvertedIndexFromCacheOrDisk(invertIndexOffset uint64, buffer []byte, invertedCache *InvertedCache) map[utils.SeriesId][]uint16 {
	var invertedIndex utils.Inverted_index
	if invertedCache != nil && len(invertedCache.Blkcache()) > 0 {
		invertedIndex = invertedCache.Get(invertIndexOffset).Mpblk()
	}
	if len(invertedIndex) == 0 {
		invertedIndex = UnserializeInvertedListBlk(invertIndexOffset, buffer).Mpblk()
	}
	return invertedIndex
}

func SearchAddrOffsetsFromCacheOrDisk(addrOffset uint64, buffer []byte, addrCache *AddrCache) map[uint64]uint16 {
	var addrOffsets map[uint64]uint16
	if addrCache != nil && len(addrCache.Blk()) > 0 {
		addrOffsets = addrCache.Get(addrOffset).Mpblk()
	}
	if len(addrOffsets) == 0 {
		addrOffsets = UnserializeAddrListBlk(addrOffset, buffer).Mpblk()
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

func MapKeyToSlices(arrMap map[utils.SeriesId]struct{}) []utils.SeriesId {
	res := make([]utils.SeriesId, 0)
	for key, _ := range arrMap {
		res = append(res, key)
	}
	return res
}

func SearchInvertedListFromChildrensOfCurrentNode(indexNode *SearchTreeNode, invertIndex2 utils.Inverted_index, buffer []byte, addrCache *AddrCache, invertedCache *InvertedCache) map[utils.SeriesId][]uint16 {
	if indexNode != nil {
		for _, child := range indexNode.Children() {
			if child.Invtdlen() > 0 {
				childInvertIndexOffset := child.InvtdInfo().IvtdblkOffset()
				childInvertedIndex := make(utils.Inverted_index)
				childInvertedIndex = SearchInvertedIndexFromCacheOrDisk(childInvertIndexOffset, buffer, invertedCache)
				if len(childInvertedIndex) > 0 {
					invertIndex2 = MergeMapsInvertLists(childInvertedIndex, invertIndex2)
				}
			}

			if child.Addrlen() > 0 {
				childAddrOffset := child.AddrInfo().AddrblkOffset()
				childAddrOffsets := SearchAddrOffsetsFromCacheOrDisk(childAddrOffset, buffer, addrCache)
				if len(childAddrOffsets) > 0 {
					var invertIndex3 = TurnAddr2InvertLists(childAddrOffsets, buffer, invertedCache)
					invertIndex2 = MergeMapsTwoInvertLists(invertIndex3, invertIndex2)
				}
			}
			invertIndex2 = SearchInvertedListFromChildrensOfCurrentNode(child, invertIndex2, buffer, addrCache, invertedCache)
		}
	}
	return invertIndex2
}

func SearchInvertedListFromChildrensOfCurrentNode2(indexNode *SearchTreeNode, arrMap map[utils.SeriesId]struct{}, buffer []byte, addrCache *AddrCache, invertedCache *InvertedCache) map[utils.SeriesId]struct{} {
	if indexNode != nil {
		for _, child := range indexNode.Children() {
			if child.Invtdlen() > 0 {
				childInvertIndexOffset := child.InvtdInfo().IvtdblkOffset()
				childInvertedIndex := make(utils.Inverted_index)
				childInvertedIndex = SearchInvertedIndexFromCacheOrDisk(childInvertIndexOffset, buffer, invertedCache)
				if len(childInvertedIndex) > 0 {
					arrMap = MergeMapsKeys(childInvertedIndex, arrMap)
				}
			}
			if child.Addrlen() > 0 {
				childAddrOffset := child.AddrInfo().AddrblkOffset()
				childAddrOffsets := SearchAddrOffsetsFromCacheOrDisk(childAddrOffset, buffer, addrCache)
				if len(childAddrOffsets) > 0 {
					arrMap = TurnAddr2InvertLists2(childAddrOffsets, buffer, invertedCache, arrMap)
				}
			}
			arrMap = SearchInvertedListFromChildrensOfCurrentNode2(child, arrMap, buffer, addrCache, invertedCache)
		}
	}
	return arrMap
}

func TurnAddr2InvertLists(addrOffsets map[uint64]uint16, buffer []byte, invertedCache *InvertedCache) utils.Inverted_index {
	var res utils.Inverted_index
	if addrOffsets == nil || len(addrOffsets) == 0 {
		return res
	}
	for addr, offset := range addrOffsets {
		invertIndex3 := make(utils.Inverted_index)
		addrInvertedIndex := SearchInvertedIndexFromCacheOrDisk(addr, buffer, invertedCache)
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

func TurnAddr2InvertLists2(addrOffsets map[uint64]uint16, buffer []byte, invertedCache *InvertedCache, arrMap map[utils.SeriesId]struct{}) map[utils.SeriesId]struct{} {
	if addrOffsets == nil || len(addrOffsets) == 0 {
		return arrMap
	}
	for addr, _ := range addrOffsets {
		addrInvertedIndex := SearchInvertedIndexFromCacheOrDisk(addr, buffer, invertedCache)
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

func MergeMapsInvertLists(map1 utils.Inverted_index, map2 utils.Inverted_index) utils.Inverted_index {
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

func DeepCopy(src utils.Inverted_index) utils.Inverted_index {
	dst := make(utils.Inverted_index)
	for key, value := range src {
		list := make([]uint16, 0)
		for i := 0; i < len(value); i++ {
			list = append(list, value[i])
		}
		dst[key] = list
	}
	return dst
}

func MergeMapsTwoInvertLists(map1 utils.Inverted_index, map2 utils.Inverted_index) utils.Inverted_index {
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
