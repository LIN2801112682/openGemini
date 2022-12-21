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
package clvIndex

import (
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramFuzzyQuery"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramMatchQuery"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramRegexQuery"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenFuzzyQuery"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenMatchQuery"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenRegexQuery"
)

type QuerySearch int32

const (
	MATCHSEARCH QuerySearch = 0
	FUZZYSEARCH QuerySearch = 1
	REGEXSEARCH QuerySearch = 2
)
const ED = 2

type QueryOption struct {
	measurement string
	fieldKey    string
	querySearch QuerySearch
	queryString string
}

func NewQueryOption(measurement string, fieldKey string, search QuerySearch, queryString string) QueryOption {
	return QueryOption{
		measurement: measurement,
		fieldKey:    fieldKey,
		querySearch: search,
		queryString: queryString,
	}
}

type CLVSearch struct {
	clvType    CLVIndexType
	buffer     []byte
	size       int64
	searchTree *mpTrie.SearchTree
	indexRoots *mpTrie.SearchTreeNode
	addrCache  *mpTrie.AddrCache
	invtdCache *mpTrie.InvertedCache
	logTree    *gramIndex.LogTree
}

func NewCLVSearch(clvType CLVIndexType) *CLVSearch {
	return &CLVSearch{
		clvType:    clvType,
		buffer:     make([]byte, 0), //[]byte{},
		size:       0,
		searchTree: nil,
		indexRoots: nil,
		addrCache:  nil,
		invtdCache: nil,
		logTree:    nil,
	}
}

func (clvSearch *CLVSearch) SearchIndexTreeFromDisk(measurement string, fieldKey string, clvType CLVIndexType) {
	if clvType == VGRAM {
		clvSearch.buffer, clvSearch.size = mpTrie.GetBytesFromFile(INDEXOUTPATH + measurement + "/" + fieldKey + "/" + "VGRAM/" + "index/" + "index0.txt")
		clvSearch.searchTree, clvSearch.addrCache, clvSearch.invtdCache = mpTrie.UnserializeGramIndexFromFile(clvSearch.buffer, clvSearch.size, 5000000, 5000000) //UnserializeGramIndexFromFile
		clvSearch.indexRoots = clvSearch.searchTree.Root()
		clvSearch.logTree = mpTrie.UnserializeLogTreeFromFile(INDEXOUTPATH + measurement + "/" + fieldKey + "/" + "VGRAM/" + "logTree/" + "log0.txt")
	} else if clvType == VTOKEN {
		clvSearch.buffer, clvSearch.size = mpTrie.GetBytesFromFile(INDEXOUTPATH + measurement + "/" + fieldKey + "/" + "VTOKEN/" + "index/" + "index0.txt")
		clvSearch.searchTree, clvSearch.addrCache, clvSearch.invtdCache = mpTrie.UnserializeTokenIndexFromFile(clvSearch.buffer, clvSearch.size, 5000000, 5000000) //UnserializeGramIndexFromFile
		clvSearch.indexRoots = clvSearch.searchTree.Root()
		clvSearch.logTree = gramIndex.NewLogTree(-1)
	}
}

func CLVSearchIndex(clvType CLVIndexType, dicType CLVDicType, queryOption QueryOption, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, buffer []byte, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache, logTree *gramIndex.LogTree) []utils.SeriesId {
	var res []utils.SeriesId
	if queryOption.querySearch == MATCHSEARCH {
		if clvType == VGRAM {
			res = MatchSearchVGramIndex(dicType, queryOption.queryString, dictionary, indexRoots, buffer, addrCache, invtdCache)
		}
		if clvType == VTOKEN {
			res = MatchSearchVTokenIndex(dicType, queryOption.queryString, dictionary, indexRoots, buffer, addrCache, invtdCache)
		}
	}
	if queryOption.querySearch == FUZZYSEARCH {
		if clvType == VGRAM {
			res = FuzzySearchVGramIndex(dicType, queryOption.queryString, dictionary, indexRoots, buffer, addrCache, invtdCache, logTree)
		}
		if clvType == VTOKEN {
			res = FuzzySearchVTokenIndex(dicType, queryOption.queryString, indexRoots, buffer, addrCache, invtdCache)
		}
	}
	if queryOption.querySearch == REGEXSEARCH {
		if clvType == VGRAM {
			res = RegexSearchVGramIndex(dicType, queryOption.queryString, dictionary, indexRoots, buffer, addrCache, invtdCache)
		}
		if clvType == VTOKEN {
			res = RegexSearchVTokenIndex(dicType, dictionary, queryOption.queryString, indexRoots, buffer, addrCache, invtdCache)
		}
	}
	return res
}

func MatchSearchVGramIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, buffer []byte, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = gramMatchQuery.MatchSearch(queryStr, dictionary.VgramDicRoot.Root(), indexRoots, QMINGRAM, buffer, addrCache, invtdCache)
	}
	if dicType == CLVL {
		res = gramMatchQuery.MatchSearch(queryStr, dictionary.VgramDicRoot.Root(), indexRoots, QMINGRAM, buffer, addrCache, invtdCache)
	}
	return res
}

func MatchSearchVTokenIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, buffer []byte, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = tokenMatchQuery.MatchSearch(queryStr, dictionary.VtokenDicRoot.Root(), indexRoots, QMINTOKEN, buffer, addrCache, invtdCache)
	}
	if dicType == CLVL {
		res = tokenMatchQuery.MatchSearch(queryStr, dictionary.VtokenDicRoot.Root(), indexRoots, QMINTOKEN, buffer, addrCache, invtdCache)
	}
	return res
}

func FuzzySearchVGramIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, buffer []byte, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache, logTree *gramIndex.LogTree) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC { //持久化在捞出来
		res = gramFuzzyQuery.FuzzyQueryGramQmaxTrie(logTree.Root(), queryStr, dictionary.VgramDicRoot.Root(), indexRoots, QMINGRAM, LOGTREEMAX, ED, buffer, addrCache, invtdCache)
	}
	if dicType == CLVL {
		res = gramFuzzyQuery.FuzzyQueryGramQmaxTrie(logTree.Root(), queryStr, dictionary.VgramDicRoot.Root(), indexRoots, QMINGRAM, LOGTREEMAX, ED, buffer, addrCache, invtdCache)
	}
	return res
}

func FuzzySearchVTokenIndex(dicType CLVDicType, queryStr string, indexRoots *mpTrie.SearchTreeNode, buffer []byte, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = tokenFuzzyQuery.FuzzySearchComparedWithES(queryStr, indexRoots, buffer, addrCache, invtdCache, ED)
	}
	if dicType == CLVL {
		res = tokenFuzzyQuery.FuzzySearchComparedWithES(queryStr, indexRoots, buffer, addrCache, invtdCache, ED)
	}
	return res
}

func RegexSearchVGramIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, buffer []byte, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = gramRegexQuery.RegexSearch(queryStr, dictionary.VgramDicRoot, QMINGRAM, indexRoots, buffer, addrCache, invtdCache)
	}
	if dicType == CLVL {
		res = gramRegexQuery.RegexSearch(queryStr, dictionary.VgramDicRoot, QMINGRAM, indexRoots, buffer, addrCache, invtdCache)
	}
	return res
}

func RegexSearchVTokenIndex(dicType CLVDicType, dictionary *CLVDictionary, queryStr string, indexRoots *mpTrie.SearchTreeNode, buffer []byte, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = tokenRegexQuery.RegexSearch(queryStr, dictionary.VtokenDicRoot.Root(), indexRoots, buffer, addrCache, invtdCache)
	}
	if dicType == CLVL {
		res = tokenRegexQuery.RegexSearch(queryStr, dictionary.VtokenDicRoot.Root(), indexRoots, buffer, addrCache, invtdCache)
	}
	return res
}
