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
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenMatchQuery"
	"regexp"
)

func RegexSearch(re string, root *tokenClvc.TrieTreeNode, indexRoot *mpTrie.SearchTreeNode, buffer []byte, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) []utils.SeriesId {
	regex, _ := regexp.Compile(re)
	sidmap := make(map[utils.SeriesId]struct{})
	result := make([]utils.SeriesId, 0)
	childrenlist := indexRoot.Children()
	for i, _ := range childrenlist {
		if regex.MatchString(childrenlist[i].Data()) {
			// match
			var invertIndex utils.Inverted_index
			var invertIndexOffset uint64
			var addrOffset uint64
			var indexNode *mpTrie.SearchTreeNode
			var invertIndex1 utils.Inverted_index
			var invertIndex2 utils.Inverted_index
			var invertIndex3 utils.Inverted_index
			invertIndexOffset, addrOffset, indexNode = tokenMatchQuery.SearchNodeAddrFromPersistentIndexTree([]string{childrenlist[i].Data()}, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
			if indexNode.Invtdlen() > 0 {
				invertIndex1 = mpTrie.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, buffer, invertedCache)
			}
			invertIndex = mpTrie.DeepCopy(invertIndex1)
			invertIndex2 = mpTrie.SearchInvertedListFromChildrensOfCurrentNode(indexNode, invertIndex2, buffer, addrCache, invertedCache)
			if indexNode.Addrlen() > 0 {
				addrOffsets := mpTrie.SearchAddrOffsetsFromCacheOrDisk(addrOffset, buffer, addrCache)
				if indexNode != nil && len(addrOffsets) > 0 {
					invertIndex3 = mpTrie.TurnAddr2InvertLists(addrOffsets, buffer, invertedCache)
				}
			}
			invertIndex = mpTrie.MergeMapsTwoInvertLists(invertIndex2, invertIndex)
			invertIndex = mpTrie.MergeMapsTwoInvertLists(invertIndex3, invertIndex)
			for k, _ := range invertIndex {
				_, isfind := sidmap[k]
				if !isfind {
					sidmap[k] = struct{}{}
				}
			}
		}
	}
	for k, _ := range sidmap {
		sid := utils.NewSeriesId(k.Id, k.Time)
		result = append(result, sid)
	}
	return result
}
