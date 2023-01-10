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
	"os"
	"strconv"
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

const SHARDBUFFER = 1000000000 //100000000

var LogIndex = 0
var BuffLogStrings = make([]utils.LogSeries, 0)
var IndexGetDataTime uint64

func (clvIndexNode *CLVIndexNode) CreateCLVIndexIfNotExists(log string, tsid uint64, timeStamp int64, indexType CLVIndexType, dicType CLVDicType, dictionary CLVDictionary, path string) {
	//fmt.Println(LogIndex)
	if LogIndex < SHARDBUFFER {
		start1 := time.Now().UnixMicro()
		BuffLogStrings = append(BuffLogStrings, utils.LogSeries{Log: log, Tsid: tsid, TimeStamp: timeStamp})
		end1 := time.Now().UnixMicro()
		IndexGetDataTime += uint64(end1-start1) / 1000
		LogIndex += 1
	}
	if LogIndex == SHARDBUFFER {
		//fmt.Println(indexType)
		fmt.Println("get index data time： ", IndexGetDataTime)
		fmt.Println("=========index data ready========")
		if indexType == VGRAM {
			clvIndexNode.CreateCLVVGramIndexIfNotExists(dicType, BuffLogStrings, dictionary.VgramDicRoot, path)
		}
		if indexType == VTOKEN {
			clvIndexNode.CreateCLVVTokenIndexIfNotExists(dicType, BuffLogStrings, dictionary.VtokenDicRoot, path)
		}
		BuffLogStrings = make([]utils.LogSeries, 0)
		LogIndex = 0
	}
}

var MemoTime uint64 = 0
var PerTime uint64 = 0

var VGramIndexPersistenceFiles []string
var VGramLogPersistenceFiles []string
var VTokenIndexPersistenceFiles []string

func (clvIndexNode *CLVIndexNode) CreateCLVVGramIndexIfNotExists(dicType CLVDicType, buffLogStrings []utils.LogSeries, vgramDicRoot *gramClvc.TrieTree, path string) {
	if dicType == CLVC {
		for i := 0; i < 4; i++ {
			start1 := time.Now().UnixMicro()
			clvIndexNode.VgramIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.GenerateIndexTree(buffLogStrings[i*50000000:(i+1)*50000000], QMINGRAM, QMAXGRAM, LOGTREEMAX, vgramDicRoot.Root())
			end1 := time.Now().UnixMicro()
			MemoTime += uint64(end1-start1) / 1000
			fmt.Println("index cost time =======")
			fmt.Println(end1 - start1)
			clvIndexNode.VgramIndexRoot.GetMemorySizeOfIndexTree()
			clvIndexNode.VgramIndexRoot.SearchTermLengthAndTermAvgLenFromIndexTree()
			clvIndexNode.LogTreeRoot.GetMemorySizeOfLogTree()
			indexPath := path + "/clvTable/" + "logs/" + "VGRAM/" + "index/"
			indexPathFile := indexPath + "index" + strconv.Itoa(i) + ".txt"
			os.MkdirAll(indexPath, os.ModePerm)
			indexFile, err := os.OpenFile(indexPathFile, os.O_CREATE|os.O_WRONLY, 0644)
			defer indexFile.Close()
			if err != nil {
				fmt.Println(err.Error())
			}
			VGramIndexPersistenceFiles = append(VGramIndexPersistenceFiles, indexPathFile)
			start2 := time.Now().UnixMicro()
			mpTrie.SerializeGramIndexToFile(clvIndexNode.VgramIndexRoot, indexPathFile)
			end2 := time.Now().UnixMicro()
			PerTime += uint64(end2-start2) / 1000
			fmt.Println("persistence cost time =======")
			fmt.Println(end2 - start2)
			clvIndexNode.VgramIndexRoot = gramIndex.NewIndexTree(QMINGRAM, QMAXGRAM)
			logPath := path + "/clvTable/" + "logs/" + "VGRAM/" + "logTree/"
			logPathFile := logPath + "log" + strconv.Itoa(i) + ".txt"
			os.MkdirAll(logPath, os.ModePerm)
			logFile, err := os.OpenFile(logPathFile, os.O_CREATE|os.O_WRONLY, 0644)
			defer logFile.Close()
			if err != nil {
				fmt.Println(err.Error())
			}
			VGramLogPersistenceFiles = append(VGramLogPersistenceFiles, logPathFile)
			mpTrie.SerializeLogTreeToFile(clvIndexNode.LogTreeRoot, logPathFile)
			clvIndexNode.LogTreeRoot = gramIndex.NewLogTree(QMAXGRAM)
			if i == 3 {
				fmt.Println("index cost all time =======", MemoTime)
				fmt.Println("persistence cost all time =======", PerTime)
			}
		}
	}
	if dicType == CLVL {
		clvIndexNode.VgramIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.GenerateIndexTree(buffLogStrings, QMINGRAM, vgramDicRoot.Qmax(), LOGTREEMAX, vgramDicRoot.Root())
	}
}

func (clvIndexNode *CLVIndexNode) CreateCLVVTokenIndexIfNotExists(dicType CLVDicType, buffLogStrings []utils.LogSeries, vtokenDicRoot *tokenClvc.TrieTree, path string) {
	if dicType == CLVC {
		for i := 0; i < 20; i++ {
			start1 := time.Now().UnixMicro()
			clvIndexNode.VtokenIndexRoot, _ = tokenIndex.GenerateIndexTree(buffLogStrings[(i)*50000000:(i+1)*50000000], QMINTOKEN, QMAXTOKEN, vtokenDicRoot.Root()) //
			end1 := time.Now().UnixMicro()
			MemoTime += uint64(end1-start1) / 1000
			fmt.Println("index cost time =======")
			fmt.Println(end1 - start1)
			clvIndexNode.VtokenIndexRoot.GetMemorySizeOfIndexTree()
			clvIndexNode.VtokenIndexRoot.SearchTermLengthAndTermAvgLenFromIndexTree()
			indexPath := path + "/clvTable/" + "logs/" + "VTOKEN/" + "index/"
			indexPathFile := indexPath + "index" + strconv.Itoa(i) + ".txt"
			os.MkdirAll(indexPath, os.ModePerm)
			indexFile, err := os.OpenFile(indexPathFile, os.O_CREATE|os.O_WRONLY, 0644)
			defer indexFile.Close()
			if err != nil {
				fmt.Println(err.Error())
			}
			VTokenIndexPersistenceFiles = append(VTokenIndexPersistenceFiles, indexPathFile)
			start2 := time.Now().UnixMicro()
			mpTrie.SerializeTokenIndexToFile(clvIndexNode.VtokenIndexRoot, indexPathFile)
			end2 := time.Now().UnixMicro()
			PerTime += uint64(end2-start2) / 1000
			fmt.Println("persistence cost time =======")
			fmt.Println(end2 - start2)
			clvIndexNode.VtokenIndexRoot = tokenIndex.NewIndexTree(QMINGRAM, QMAXGRAM)
			if i == 19 {
				fmt.Println("index cost all time =======", MemoTime)
				fmt.Println("persistence cost all time =======", PerTime)
			}
		}
	}
	if dicType == CLVL {
		clvIndexNode.VtokenIndexRoot, _ = tokenIndex.GenerateIndexTree(buffLogStrings, QMINTOKEN, vtokenDicRoot.Qmax(), vtokenDicRoot.Root())
	}
}
