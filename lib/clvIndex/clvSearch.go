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
	"os"
)

type QuerySearch int32

const (
	MATCHSEARCH QuerySearch = 0
	FUZZYSEARCH QuerySearch = 1
	REGEXSEARCH QuerySearch = 2
)

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
	filePtr    map[int]*os.File
	fileNames  []string
	searchTree *mpTrie.SearchTree
	indexRoots *mpTrie.SearchTreeNode
	addrCache  *mpTrie.AddrCache
	invtdCache *mpTrie.InvertedCache
	logTree    *gramIndex.LogTree
	tokenMap   map[string][]*mpTrie.SearchTreeNode
}

func (clvSearch *CLVSearch) TokenMap() map[string][]*mpTrie.SearchTreeNode {
	return clvSearch.tokenMap
}

func (clvSearch *CLVSearch) SetTokenMap(tokenMap map[string][]*mpTrie.SearchTreeNode) {
	clvSearch.tokenMap = tokenMap
}

func (clvSearch *CLVSearch) ClvType() CLVIndexType {
	return clvSearch.clvType
}

func (clvSearch *CLVSearch) SetClvType(clvType CLVIndexType) {
	clvSearch.clvType = clvType
}

func (clvSearch *CLVSearch) FilePtr() map[int]*os.File {
	return clvSearch.filePtr
}

func (clvSearch *CLVSearch) SetFilePtr(filePtr map[int]*os.File) {
	clvSearch.filePtr = filePtr
}

func (clvSearch *CLVSearch) FileNames() []string {
	return clvSearch.fileNames
}

func (clvSearch *CLVSearch) SetFileNames(fileNames []string) {
	clvSearch.fileNames = fileNames
}

func (clvSearch *CLVSearch) SearchTree() *mpTrie.SearchTree {
	return clvSearch.searchTree
}

func (clvSearch *CLVSearch) SetSearchTree(searchTree *mpTrie.SearchTree) {
	clvSearch.searchTree = searchTree
}

func (clvSearch *CLVSearch) IndexRoots() *mpTrie.SearchTreeNode {
	return clvSearch.indexRoots
}

func (clvSearch *CLVSearch) SetIndexRoots(indexRoots *mpTrie.SearchTreeNode) {
	clvSearch.indexRoots = indexRoots
}

func (clvSearch *CLVSearch) AddrCache() *mpTrie.AddrCache {
	return clvSearch.addrCache
}

func (clvSearch *CLVSearch) SetAddrCache(addrCache *mpTrie.AddrCache) {
	clvSearch.addrCache = addrCache
}

func (clvSearch *CLVSearch) InvtdCache() *mpTrie.InvertedCache {
	return clvSearch.invtdCache
}

func (clvSearch *CLVSearch) SetInvtdCache(invtdCache *mpTrie.InvertedCache) {
	clvSearch.invtdCache = invtdCache
}

func (clvSearch *CLVSearch) LogTree() *gramIndex.LogTree {
	return clvSearch.logTree
}

func (clvSearch *CLVSearch) SetLogTree(logTree *gramIndex.LogTree) {
	clvSearch.logTree = logTree
}

func NewCLVSearch(clvType CLVIndexType) *CLVSearch {
	return &CLVSearch{
		clvType:    clvType,
		filePtr:    make(map[int]*os.File, 0),
		fileNames:  make([]string, 0),
		searchTree: nil,
		indexRoots: nil,
		addrCache:  nil,
		invtdCache: nil,
		logTree:    nil,
		tokenMap:   make(map[string][]*mpTrie.SearchTreeNode),
	}
}

func (clvSearch *CLVSearch) SearchIndexTreeFromDisk(clvType CLVIndexType, measurement string, fieldKey string, path string) {
	if clvType == VGRAM {
		var err error
		if len(VGramIndexPersistenceFiles) > 0 {
			clvSearch.fileNames = VGramIndexPersistenceFiles
		} else {
			var s []string
			clvSearch.fileNames, err = utils.GetAllFile(path+"/"+measurement+"/"+fieldKey+"/"+"VGRAM/"+"index/", s)
		}
		if err == nil {
			clvSearch.searchTree, clvSearch.filePtr, clvSearch.addrCache, clvSearch.invtdCache = mpTrie.DecodeGramIndexFromMultiFile(clvSearch.fileNames, 50000000, 50000000)
			clvSearch.indexRoots = clvSearch.searchTree.Root()
			var s []string
			logFileName, _ := utils.GetAllFile(path+"/"+measurement+"/"+fieldKey+"/"+"VGRAM/"+"logTree/", s)
			clvSearch.logTree = mpTrie.DecodeLogTreeFromMultiFiles(logFileName, LOGTREEMAX)
		}
	} else if clvType == VTOKEN {
		var err error
		if len(VTokenIndexPersistenceFiles) > 0 {
			clvSearch.fileNames = VTokenIndexPersistenceFiles
		} else {
			var s []string
			clvSearch.fileNames, err = utils.GetAllFile(path+"/"+measurement+"/"+fieldKey+"/"+"VTOKEN/"+"index/", s)
		}
		if err == nil {
			clvSearch.searchTree, clvSearch.filePtr, clvSearch.addrCache, clvSearch.invtdCache = mpTrie.DecodeTokenIndexFromMultiFile(clvSearch.fileNames, 50000000, 50000000)
			clvSearch.indexRoots = clvSearch.searchTree.Root()
			clvSearch.tokenMap = clvSearch.indexRoots.GetGramMap(REGEX_Q)
			clvSearch.indexRoots.TokenPrefixGrams(PREFIXLEN, ED)
			clvSearch.logTree = gramIndex.NewLogTree(-1)
		}
	}
}

func CLVSearchIndex(clvType CLVIndexType, dicType CLVDicType, queryOption QueryOption, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache, logTree *gramIndex.LogTree, tokenMap map[string][]*mpTrie.SearchTreeNode) map[utils.SeriesId]struct{} {
	var res map[utils.SeriesId]struct{}
	if queryOption.querySearch == MATCHSEARCH {
		if clvType == VGRAM {
			res = MatchSearchVGramIndex(dicType, queryOption.queryString, dictionary, indexRoots, filePtr, addrCache, invtdCache)
		}
		if clvType == VTOKEN {
			res = MatchSearchVTokenIndex(dicType, queryOption.queryString, dictionary, indexRoots, filePtr, addrCache, invtdCache)
		}
	}
	if queryOption.querySearch == FUZZYSEARCH {
		if clvType == VGRAM {
			res = FuzzySearchVGramIndex(dicType, queryOption.queryString, dictionary, indexRoots, filePtr, addrCache, invtdCache, logTree)
		}
		if clvType == VTOKEN {
			res = FuzzySearchVTokenIndex(dicType, queryOption.queryString, indexRoots, filePtr, addrCache, invtdCache)
		}
	}
	if queryOption.querySearch == REGEXSEARCH {
		if clvType == VGRAM {
			res = RegexSearchVGramIndex(dicType, queryOption.queryString, dictionary, indexRoots, filePtr, addrCache, invtdCache)
		}
		if clvType == VTOKEN {
			res = RegexSearchVTokenIndex(dicType, queryOption.queryString, indexRoots, filePtr, addrCache, invtdCache, tokenMap)
		}
	}
	return res
}

func MatchSearchVGramIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	var res = make(map[utils.SeriesId]struct{})
	if dicType == CLVC {
		res = gramMatchQuery.MatchSearch(queryStr, dictionary.VgramDicRoot.Root(), indexRoots, QMINGRAM, filePtr, addrCache, invtdCache)
	}
	if dicType == CLVL {
		res = gramMatchQuery.MatchSearch(queryStr, dictionary.VgramDicRoot.Root(), indexRoots, QMINGRAM, filePtr, addrCache, invtdCache)
	}
	return res
}

func MatchSearchVTokenIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	var res = make(map[utils.SeriesId]struct{})
	if dicType == CLVC {
		res = tokenMatchQuery.MatchSearch(queryStr, dictionary.VtokenDicRoot.Root(), indexRoots, QMINTOKEN, filePtr, addrCache, invtdCache)
	}
	if dicType == CLVL {
		res = tokenMatchQuery.MatchSearch(queryStr, dictionary.VtokenDicRoot.Root(), indexRoots, QMINTOKEN, filePtr, addrCache, invtdCache)
	}
	return res
}

func FuzzySearchVGramIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache, logTree *gramIndex.LogTree) map[utils.SeriesId]struct{} {
	var res = make(map[utils.SeriesId]struct{})
	if dicType == CLVC {
		res = gramFuzzyQuery.FuzzyQueryGramQmaxTries(logTree.Root(), queryStr, dictionary.VgramDicRoot.Root(), indexRoots, QMINGRAM, LOGTREEMAX, ED, filePtr, addrCache, invtdCache)
	}
	if dicType == CLVL {
		res = gramFuzzyQuery.FuzzyQueryGramQmaxTries(logTree.Root(), queryStr, dictionary.VgramDicRoot.Root(), indexRoots, QMINGRAM, LOGTREEMAX, ED, filePtr, addrCache, invtdCache)
	}
	return res
}

func FuzzySearchVTokenIndex(dicType CLVDicType, queryStr string, indexRoots *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	var res = make(map[utils.SeriesId]struct{})
	if dicType == CLVC {
		res = tokenFuzzyQuery.FuzzyTokenQueryTries(queryStr, indexRoots, filePtr, addrCache, invtdCache, ED, PREFIXLEN)
	}
	if dicType == CLVL {
		res = tokenFuzzyQuery.FuzzyTokenQueryTries(queryStr, indexRoots, filePtr, addrCache, invtdCache, ED, PREFIXLEN)
	}
	return res
}

func RegexSearchVGramIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, indexRoots *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	var res = make(map[utils.SeriesId]struct{})
	if dicType == CLVC {
		res = gramRegexQuery.RegexSearch(queryStr, dictionary.VgramDicRoot, QMINGRAM, indexRoots, filePtr, addrCache, invtdCache)
	}
	if dicType == CLVL {
		res = gramRegexQuery.RegexSearch(queryStr, dictionary.VgramDicRoot, QMINGRAM, indexRoots, filePtr, addrCache, invtdCache)
	}
	return res
}

func RegexSearchVTokenIndex(dicType CLVDicType, queryStr string, indexRoots *mpTrie.SearchTreeNode, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invtdCache *mpTrie.InvertedCache, tokenMap map[string][]*mpTrie.SearchTreeNode) map[utils.SeriesId]struct{} {
	var res = make(map[utils.SeriesId]struct{})
	if dicType == CLVC {
		res = tokenRegexQuery.RegexSearch(queryStr, indexRoots, filePtr, addrCache, invtdCache, tokenMap)
	}
	if dicType == CLVL {
		res = tokenRegexQuery.RegexSearch(queryStr, indexRoots, filePtr, addrCache, invtdCache, tokenMap)
	}
	return res
}