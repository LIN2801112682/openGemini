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
/*
	This module is the specific architecture design of CLV index.
	The key of indexTreeMap structure is a column of a table, and value is its corresponding dictionary and index.
	IndexType is the index type. DicAndIndex contains specific dictionaries and indexes
	The CreateCLVIndex function is used to create a dictionary based on log information, and then create an index using the dictionary.
	The CLVSearch function is used to query the index according to the table name, column name and query options, and get the result set containing the ID and timestamp.
*/
package clvIndex

import (
	"github.com/openGemini/openGemini/lib/utils"
)

type CLVIndex struct {
	indexTreeMap map[MeasurementAndFieldKey]*CLVIndexNode
	indexType    CLVIndexType
	search       *CLVSearch
	path         string
}

func (clvIndex *CLVIndex) IndexTreeMap() map[MeasurementAndFieldKey]*CLVIndexNode {
	return clvIndex.indexTreeMap
}

func (clvIndex *CLVIndex) SetIndexTreeMap(indexTreeMap map[MeasurementAndFieldKey]*CLVIndexNode) {
	clvIndex.indexTreeMap = indexTreeMap
}

func (clvIndex *CLVIndex) IndexType() CLVIndexType {
	return clvIndex.indexType
}

func (clvIndex *CLVIndex) SetIndexType(indexType CLVIndexType) {
	clvIndex.indexType = indexType
}

func (clvIndex *CLVIndex) Search() *CLVSearch {
	return clvIndex.search
}

func (clvIndex *CLVIndex) SetSearch(search *CLVSearch) {
	clvIndex.search = search
}

func (clvIndex *CLVIndex) Path() string {
	return clvIndex.path
}

func (clvIndex *CLVIndex) SetPath(path string) {
	clvIndex.path = path
}

func NewCLVIndex(indexType CLVIndexType, path string) *CLVIndex {
	return &CLVIndex{
		indexTreeMap: make(map[MeasurementAndFieldKey]*CLVIndexNode),
		indexType:    indexType,
		search:       NewCLVSearch(indexType),
		path:         path,
	}
}

/*
	There are two types of dictionaries, CLVC and CLVL. CLVC is a configuration dictionary based on a batch of data, and CLVL is a learning dictionary based on a batch of data and query load.
*/

type CLVDicType int32

const (
	CLVC CLVDicType = 0
	CLVL CLVDicType = 1
)

/*
	There are two types of indexes, namely VGRAM and VTOKEN.
	The former is an index item constructed according to character division, and is aligned with the NGram tokenizer of ES;
	the latter is an index item constructed based on word division, which is based on the standard segmentation of ES.
*/

type CLVIndexType int32

const (
	VGRAM  CLVIndexType = 0
	VTOKEN CLVIndexType = 1
)

type MeasurementAndFieldKey struct {
	measurementName string
	fieldKey        string
}

func NewMeasurementAndFieldKey(measurementName string, fieldKey string) MeasurementAndFieldKey {
	return MeasurementAndFieldKey{
		measurementName: measurementName,
		fieldKey:        fieldKey,
	}
}

func (clvIndex *CLVIndex) CreateCLVIndex(log string, tsid uint64, timeStamp int64, measurement string, fieldName string) {
	measurementAndFieldKey := NewMeasurementAndFieldKey(measurement, fieldName)
	if DicIndex != MAXDICBUFFER {
		clvIndex.indexTreeMap[measurementAndFieldKey].dic.CreateDictionaryIfNotExists(log, tsid, timeStamp, clvIndex.indexType, clvIndex.path)
	}
	clvIndex.indexTreeMap[measurementAndFieldKey].CreateCLVIndexIfNotExists(log, tsid, timeStamp)
}

func (clvIndex *CLVIndex) CLVSearch(measurementName string, fieldKey string, queryType QuerySearch, queryStr string) (map[utils.SeriesId]struct{}, []utils.SeriesId) {
	var resMap = make(map[utils.SeriesId]struct{})
	var resSlice = make([]utils.SeriesId, 0)
	option := NewQueryOption(measurementName, fieldKey, queryType, queryStr)
	measurementAndFieldKey := NewMeasurementAndFieldKey(measurementName, fieldKey)
	if _, ok := clvIndex.indexTreeMap[measurementAndFieldKey]; ok {
		dic := clvIndex.indexTreeMap[measurementAndFieldKey].dic
		indexType := clvIndex.indexType
		if len(clvIndex.search.fileNames) == 0 {
			clvIndex.search.SearchIndexTreeFromDisk(clvIndex.indexType, measurementName, fieldKey, clvIndex.path)
		}
		resMap, resSlice = CLVSearchIndex(indexType, dic.DicType, option, dic, clvIndex.search.indexRoots, clvIndex.search.filePtr, clvIndex.search.addrCache, clvIndex.search.invtdCache, clvIndex.search.logTree, clvIndex.search.tokenMap, clvIndex.search.shortFuzzyIndex, clvIndex.search.longFuzzyIndex, clvIndex.search.gramShortFuzzyIndex, clvIndex.search.gramLongFuzzyIndex)
	} else {
		resMap = nil
		resSlice = nil
	}
	return resMap, resSlice
}
