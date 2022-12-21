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
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
	"time"
)

type CLVIndexNode struct {
	VgramIndexRoot  *gramIndex.IndexTree
	LogTreeRoot     *gramIndex.LogTree
	VtokenIndexRoot *tokenIndex.IndexTree
}

func NewCLVIndexNode() *CLVIndexNode {
	return &CLVIndexNode{
		VgramIndexRoot:  gramIndex.NewIndexTree(QMINGRAM, QMAXGRAM),
		LogTreeRoot:     gramIndex.NewLogTree(QMAXGRAM),
		VtokenIndexRoot: tokenIndex.NewIndexTree(QMINTOKEN, QMAXTOKEN),
	}
}

/*
	SHARDBUFFER is the number of data of a SHARD, LogIndex is a counter, and BuffLogStrings is used to store all the data of a SHARD, which is used to build indexes in batches.
*/

const SHARDBUFFER = 500000

var LogIndex = 0
var BuffLogStrings = make([]utils.LogSeries, 0)

func (clvIndexNode *CLVIndexNode) CreateCLVIndexIfNotExists(log string, tsid uint64, timeStamp int64, indexType CLVIndexType, dicType CLVDicType, dictionary CLVDictionary) {
	//fmt.Println(LogIndex)
	if LogIndex < SHARDBUFFER {
		BuffLogStrings = append(BuffLogStrings, utils.LogSeries{Log: log, Tsid: tsid, TimeStamp: timeStamp})
		LogIndex += 1
	}
	if LogIndex == SHARDBUFFER {
		if indexType == VGRAM {
			clvIndexNode.CreateCLVVGramIndexIfNotExists(dicType, BuffLogStrings, dictionary.VgramDicRoot)
		}
		if indexType == VTOKEN {
			clvIndexNode.CreateCLVVTokenIndexIfNotExists(dicType, BuffLogStrings, dictionary.VtokenDicRoot)
		}
		BuffLogStrings = make([]utils.LogSeries, 0)
		LogIndex = 0
	}
}

const INDEXOUTPATH = "../../lib/persistence/"

func (clvIndexNode *CLVIndexNode) CreateCLVVGramIndexIfNotExists(dicType CLVDicType, buffLogStrings []utils.LogSeries, vgramDicRoot *gramClvc.TrieTree) {
	if dicType == CLVC {
		start111 := time.Now().UnixMicro()
		clvIndexNode.VgramIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.GenerateIndexTree(buffLogStrings, QMINGRAM, QMAXGRAM, LOGTREEMAX, vgramDicRoot.Root())
		//clvIndexNode.VgramIndexRoot.PrintIndexTree()
		end111 := time.Now().UnixMicro()
		fmt.Println("index cost time =======")
		fmt.Println(end111 - start111)
		clvIndexNode.VgramIndexRoot.GetMemorySizeOfIndexTree()
		clvIndexNode.VgramIndexRoot.SearchTermLengthAndTermAvgLenFromIndexTree()
		indexPath := INDEXOUTPATH + "clvTable/" + "logs/" + "VGRAM/" + "index/" + "index0.txt"
		start222 := time.Now().UnixMicro()
		mpTrie.SerializeGramIndexToFile(clvIndexNode.VgramIndexRoot, indexPath)
		end222 := time.Now().UnixMicro()
		fmt.Println("persistence cost time =======")
		fmt.Println(end222 - start222)
		clvIndexNode.VgramIndexRoot = gramIndex.NewIndexTree(QMINGRAM, QMAXGRAM)
		mpTrie.SerializeLogTreeToFile(clvIndexNode.LogTreeRoot, INDEXOUTPATH+"clvTable/"+"logs/"+"VGRAM/"+"logTree/"+"log0.txt")
		clvIndexNode.LogTreeRoot = gramIndex.NewLogTree(QMAXGRAM)
	}
	if dicType == CLVL {
		clvIndexNode.VgramIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.GenerateIndexTree(buffLogStrings, QMINGRAM, vgramDicRoot.Qmax(), LOGTREEMAX, vgramDicRoot.Root())
	}
}

func (clvIndexNode *CLVIndexNode) CreateCLVVTokenIndexIfNotExists(dicType CLVDicType, buffLogStrings []utils.LogSeries, vtokenDicRoot *tokenClvc.TrieTree) {
	if dicType == CLVC {
		start1 := time.Now().UnixMicro()
		clvIndexNode.VtokenIndexRoot, _ = tokenIndex.GenerateIndexTree(buffLogStrings, QMINTOKEN, QMAXTOKEN, vtokenDicRoot.Root())
		end1 := time.Now().UnixMicro()
		fmt.Println("index cost time =======")
		fmt.Println(end1 - start1)
		//clvIndexNode.VtokenClvcIndexRoot.PrintIndexTree()
		clvIndexNode.VtokenIndexRoot.GetMemorySizeOfIndexTree()
		clvIndexNode.VtokenIndexRoot.SearchTermLengthAndTermAvgLenFromIndexTree()
		indexPath := INDEXOUTPATH + "clvTable/" + "logs/" + "VTOKEN/" + "index/" + "index0.txt"
		start2 := time.Now().UnixMicro()
		mpTrie.SerializeTokenIndexToFile(clvIndexNode.VtokenIndexRoot, indexPath)
		end2 := time.Now().UnixMicro()
		fmt.Println("persistence cost time =======")
		fmt.Println(end2 - start2)
		clvIndexNode.VtokenIndexRoot = tokenIndex.NewIndexTree(QMINGRAM, QMAXGRAM)
	}
	if dicType == CLVL {
		clvIndexNode.VtokenIndexRoot, _ = tokenIndex.GenerateIndexTree(buffLogStrings, QMINTOKEN, vtokenDicRoot.Qmax(), vtokenDicRoot.Root())
	}
}
