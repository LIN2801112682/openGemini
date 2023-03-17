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
	"sync"
	"time"
)

type semaphore int

const (
	update semaphore = iota
	close
)

const SHARDBUFFER = 500000 //100000000

type CLVIndexNode struct {
	VgramIndexRoot  *gramIndex.IndexTree
	LogTreeRoot     *gramIndex.LogTree
	VtokenIndexRoot *tokenIndex.IndexTree

	dataSignal             chan semaphore
	dataLock               sync.Mutex
	dataLen                int
	dataBuf                []utils.LogSeries
	path                   string
	measurementAndFieldKey MeasurementAndFieldKey
	dicType                CLVDicType
	dic                    *CLVDictionary
	indexType              CLVIndexType
}

func (clvIndexNode *CLVIndexNode) Path() string {
	return clvIndexNode.path
}

func (clvIndexNode *CLVIndexNode) SetPath(path string) {
	clvIndexNode.path = path
}

func (clvIndexNode *CLVIndexNode) MeasurementAndFieldKey() MeasurementAndFieldKey {
	return clvIndexNode.measurementAndFieldKey
}

func (clvIndexNode *CLVIndexNode) SetMeasurementAndFieldKey(measurementAndFieldKey MeasurementAndFieldKey) {
	clvIndexNode.measurementAndFieldKey = measurementAndFieldKey
}

func (clvIndexNode *CLVIndexNode) DicType() CLVDicType {
	return clvIndexNode.dicType
}

func (clvIndexNode *CLVIndexNode) SetDicType(dicType CLVDicType) {
	clvIndexNode.dicType = dicType
}

func (clvIndexNode *CLVIndexNode) Dic() *CLVDictionary {
	return clvIndexNode.dic
}

func (clvIndexNode *CLVIndexNode) SetDic(dic *CLVDictionary) {
	clvIndexNode.dic = dic
}

func (clvIndexNode *CLVIndexNode) IndexType() CLVIndexType {
	return clvIndexNode.indexType
}

func (clvIndexNode *CLVIndexNode) SetIndexType(indexType CLVIndexType) {
	clvIndexNode.indexType = indexType
}

func NewCLVIndexNode(indexType CLVIndexType, dic *CLVDictionary, measurementAndFieldKey MeasurementAndFieldKey, path string) *CLVIndexNode {
	clvIndex := &CLVIndexNode{
		VgramIndexRoot:  gramIndex.NewIndexTree(QMINGRAM, QMAXGRAM),
		LogTreeRoot:     gramIndex.NewLogTree(QMAXGRAM),
		VtokenIndexRoot: tokenIndex.NewIndexTree(QMINTOKEN, QMAXTOKEN),

		dataSignal:             make(chan semaphore),
		dataBuf:                make([]utils.LogSeries, 0, SHARDBUFFER),
		path:                   path,
		measurementAndFieldKey: measurementAndFieldKey,
		dicType:                CLVC,
		dic:                    dic,
		indexType:              indexType,
	}
	clvIndex.Open()
	return clvIndex
}

func (clvIndexNode *CLVIndexNode) Open() {
	go clvIndexNode.updateClvIndexRoutine()
}

func (clvIndexNode *CLVIndexNode) Close() {
	clvIndexNode.dataSignal <- close
}

func (clvIndexNode *CLVIndexNode) updateClvIndexRoutine() {
	for {
		select {
		case _, ok := <-clvIndexNode.dataSignal:
			if !ok {
				return
			}
			clvIndexNode.updateClvIndex()
		}
	}
}

func (clvIndexNode *CLVIndexNode) updateClvIndex() {
	var logbuf []utils.LogSeries

	clvIndexNode.dataLock.Lock()
	if clvIndexNode.dataLen == 0 {
		clvIndexNode.dataLock.Unlock()
		return
	}
	logbuf = clvIndexNode.dataBuf
	clvIndexNode.dataBuf = make([]utils.LogSeries, 0, SHARDBUFFER)
	clvIndexNode.dataLen = 0
	clvIndexNode.dataLock.Unlock()

	fmt.Println("========= go to build the index ========")
	if clvIndexNode.indexType == VTOKEN {
		clvIndexNode.CreateCLVVTokenIndexIfNotExists(clvIndexNode.dicType, logbuf, clvIndexNode.dic.VtokenDicRoot, clvIndexNode.path)
	} else if clvIndexNode.indexType == VGRAM {
		clvIndexNode.CreateCLVVGramIndexIfNotExists(clvIndexNode.dicType, logbuf, clvIndexNode.dic.VgramDicRoot, clvIndexNode.path)
	}

}

//func (clvIndexNode *CLVIndexNode) CreateCLVIndexIfNotExists(log string, tsid uint64, timeStamp int64) {
func (clvIndexNode *CLVIndexNode) CreateCLVIndexIfNotExists(log string, tsid uint64, timeStamp int64) {
	clvIndexNode.dataLock.Lock()
	defer clvIndexNode.dataLock.Unlock()

	if clvIndexNode.dataLen < SHARDBUFFER {
		clvIndexNode.dataBuf = append(clvIndexNode.dataBuf, utils.LogSeries{Log: log, Tsid: tsid, TimeStamp: timeStamp})
		clvIndexNode.dataLen += 1
	}
	if clvIndexNode.dataLen%SHARDBUFFER == 0 {
		fmt.Println(clvIndexNode.dataLen/SHARDBUFFER, "5000w data ready, startTime :", time.Now())
	}
	if clvIndexNode.dataLen >= SHARDBUFFER { // == todo
		clvIndexNode.dataSignal <- update
		fmt.Println("========= index data ready ========")
	}
}

var PersitenceId = 0
var MemoTime uint64 = 0
var PerTime uint64 = 0

var VGramIndexPersistenceFiles []string
var VGramLogPersistenceFiles []string
var VTokenIndexPersistenceFiles []string

func (clvIndexNode *CLVIndexNode) CreateCLVVGramIndexIfNotExists(dicType CLVDicType, buffLogStrings []utils.LogSeries, vgramDicRoot *gramClvc.TrieTree, path string) {
	if dicType == CLVC {
		start1 := time.Now().UnixMicro()
		clvIndexNode.VgramIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.GenerateIndexTree(buffLogStrings, QMINGRAM, QMAXGRAM, LOGTREEMAX, vgramDicRoot.Root())
		end1 := time.Now().UnixMicro()
		MemoTime += uint64(end1-start1) / 1000
		fmt.Println("index cost time(ms): ", float64((end1-start1)/1000))
		clvIndexNode.VgramIndexRoot.GetMemorySizeOfIndexTree()
		clvIndexNode.VgramIndexRoot.SearchTermLengthAndTermAvgLenFromIndexTree()
		clvIndexNode.LogTreeRoot.GetMemorySizeOfLogTree()
		indexPath := path + "/clvTable/" + "logs/" + "VGRAM/" + "index/"
		indexPathFile := indexPath + "index" + strconv.Itoa(PersitenceId) + ".txt"
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
		fmt.Println("persistence cost time(ms): ", float64((end2-start2)/1000))
		clvIndexNode.VgramIndexRoot = gramIndex.NewIndexTree(QMINGRAM, QMAXGRAM)
		logPath := path + "/clvTable/" + "logs/" + "VGRAM/" + "logTree/"
		logPathFile := logPath + "log" + strconv.Itoa(PersitenceId) + ".txt"
		os.MkdirAll(logPath, os.ModePerm)
		logFile, err := os.OpenFile(logPathFile, os.O_CREATE|os.O_WRONLY, 0644)
		defer logFile.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
		VGramLogPersistenceFiles = append(VGramLogPersistenceFiles, logPathFile)
		mpTrie.SerializeLogTreeToFile(clvIndexNode.LogTreeRoot, logPathFile)
		clvIndexNode.LogTreeRoot = gramIndex.NewLogTree(QMAXGRAM)
		if PersitenceId == 0 {
			fmt.Println("index cost all time =======", MemoTime)
			fmt.Println("persistence cost all time =======", PerTime)
		}
		PersitenceId += 1
	}
	if dicType == CLVL {
		clvIndexNode.VgramIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.GenerateIndexTree(buffLogStrings, QMINGRAM, vgramDicRoot.Qmax(), LOGTREEMAX, vgramDicRoot.Root())
	}
}

func (clvIndexNode *CLVIndexNode) CreateCLVVTokenIndexIfNotExists(dicType CLVDicType, buffLogStrings []utils.LogSeries, vtokenDicRoot *tokenClvc.TrieTree, path string) {
	if dicType == CLVC {
		start1 := time.Now().UnixMicro()
		clvIndexNode.VtokenIndexRoot, _ = tokenIndex.GenerateIndexTree(buffLogStrings, QMINTOKEN, QMAXTOKEN, vtokenDicRoot.Root())
		end1 := time.Now().UnixMicro()
		MemoTime += uint64(end1-start1) / 1000
		fmt.Println("index cost time(ms): ", float64((end1-start1)/1000))
		clvIndexNode.VtokenIndexRoot.GetMemorySizeOfIndexTree()
		clvIndexNode.VtokenIndexRoot.SearchTermLengthAndTermAvgLenFromIndexTree()
		indexPath := path + "/clvTable/" + "logs/" + "VTOKEN/" + "index/"
		indexPathFile := indexPath + "index" + strconv.Itoa(PersitenceId) + ".txt"
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
		fmt.Println("persistence cost time(ms): ", float64((end2-start2)/1000))
		clvIndexNode.VtokenIndexRoot = tokenIndex.NewIndexTree(QMINGRAM, QMAXGRAM)
		if PersitenceId == 19 {
			fmt.Println("index cost all time =======", MemoTime)
			fmt.Println("persistence cost all time =======", PerTime)
		}
		PersitenceId += 1
	}
	if dicType == CLVL {
		clvIndexNode.VtokenIndexRoot, _ = tokenIndex.GenerateIndexTree(buffLogStrings, QMINTOKEN, vtokenDicRoot.Qmax(), vtokenDicRoot.Root())
	}
}
