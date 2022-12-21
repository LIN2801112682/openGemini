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
package tokenIndex

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
	"unsafe"
)

type IndexTreeNode struct {
	data string
	frequency     int
	children      map[int]*IndexTreeNode
	isleaf        bool
	invertedIndex utils.Inverted_index
	addrOffset    map[*IndexTreeNode]uint16
}

func (node *IndexTreeNode) Children() map[int]*IndexTreeNode {
	return node.children
}

func (node *IndexTreeNode) SetChildren(children map[int]*IndexTreeNode) {
	node.children = children
}

func (node *IndexTreeNode) Data() string {
	return node.data
}

func (node *IndexTreeNode) SetData(data string) {
	node.data = data
}

func (node *IndexTreeNode) Isleaf() bool {
	return node.isleaf
}

func (node *IndexTreeNode) SetIsleaf(isleaf bool) {
	node.isleaf = isleaf
}

func (node *IndexTreeNode) InvertedIndex() utils.Inverted_index {
	return node.invertedIndex
}

func (node *IndexTreeNode) SetInvertedIndex(invertedIndex utils.Inverted_index) {
	node.invertedIndex = invertedIndex
}

func (node *IndexTreeNode) AddrOffset() map[*IndexTreeNode]uint16 {
	return node.addrOffset
}

func (node *IndexTreeNode) SetAddrOffset(addrOffset map[*IndexTreeNode]uint16) {
	node.addrOffset = addrOffset
}

func (node *IndexTreeNode) Frequency() int {
	return node.frequency
}

func (node *IndexTreeNode) SetFrequency(frequency int) {
	node.frequency = frequency
}

func NewIndexTreeNode(data string) *IndexTreeNode {
	return &IndexTreeNode{
		data:          data,
		frequency:     1,
		isleaf:        false,
		children:      make(map[int]*IndexTreeNode),
		invertedIndex: make(map[utils.SeriesId][]uint16),
		addrOffset:    make(map[*IndexTreeNode]uint16),
	}
}

func GetIndexNode(children map[int]*IndexTreeNode, str string) int {
	if children[utils.StringToHashCode(str)] != nil {
		return utils.StringToHashCode(str)
	}
	return -1
}

func (node *IndexTreeNode) InsertPosArrToInvertedIndexMap(sid utils.SeriesId, position uint16) {
	//倒排列表数组中找到sid的invertedIndex，把position加入到invertedIndex中的posArray中去
	node.invertedIndex[sid] = append(node.invertedIndex[sid], position)
}

func (node *IndexTreeNode) InsertSidAndPosArrToInvertedIndexMap(sid utils.SeriesId, position uint16) {
	posArray := []uint16{}
	posArray = append(posArray, position)
	node.invertedIndex[sid] = posArray
}

func (node *IndexTreeNode) PrintIndexTreeNode(level int) {
	fmt.Println()
	for i := 0; i < level; i++ {
		fmt.Print("      ")
	}
	//fmt.Print(node.data, " - ", " - ", node.isleaf, " - ", node.invertedIndex, " - ", node.addrOffset)
	fmt.Print(node.data, " - ", node.frequency, " - ", node.isleaf, " - ", len(node.invertedIndex), " - ", len(node.addrOffset))
	for _, child := range node.children {
		child.PrintIndexTreeNode(level + 1)
	}
}

func InsertInvertedIndexPos(invertedIndex utils.Inverted_index, sid utils.SeriesId, position uint16) {
	//倒排列表数组中找到sid的invertedIndex，把position加入到invertedIndex中的posArray中去
	invertedIndex[sid] = append(invertedIndex[sid], position)
}

func InsertInvertedIndexList(node *IndexTreeNode, sid utils.SeriesId, position uint16) {
	posArray := []uint16{}
	posArray = append(posArray, position)
	node.invertedIndex[sid] = posArray
}

var TheoreticalMemoUsed uint64 = 0
var ExactMemoUsed uint64 = 0

const NODEFREQBYTE = 4
const NODEDATABYTE = 1
const NODEISLEAFBYTE = 1

const CHILDMAPBYTE = 4

const SIDBYTE = 16
const POSBYTE = 2

const ADDRBYTE = 1
const OFFSETBYTE = 2

var InvertedSize uint64 = 0

var AddrSize uint64 = 0

func (node *IndexTreeNode) GetMemorySizeOfIndexTreeTheoretical(level int) { //unsafe.sizeof
	TheoreticalMemoUsed += (NODEISLEAFBYTE + NODEDATABYTE + NODEFREQBYTE)
	if len(node.children) > 0 {
		TheoreticalMemoUsed += uint64(len(node.children) * CHILDMAPBYTE)
	}
	invertedIndex := node.invertedIndex
	if len(invertedIndex) > 0 {
		InvertedSize += uint64(len(invertedIndex))
		for _, v := range invertedIndex {
			TheoreticalMemoUsed += uint64(SIDBYTE + len(v)*POSBYTE)
		}
	}
	addrOffset := node.addrOffset
	if len(addrOffset) > 0 {
		AddrSize += uint64(len(addrOffset))
		TheoreticalMemoUsed += uint64(len(addrOffset) * (ADDRBYTE + OFFSETBYTE))
	}
	for _, child := range node.children {
		child.GetMemorySizeOfIndexTreeTheoretical(level + 1)
	}
}

func (node *IndexTreeNode) GetMemorySizeOfIndexTreeExact(level int) { //unsafe.sizeof
	ExactMemoUsed += uint64(unsafe.Sizeof(node))
	if len(node.children) > 0 {
		ExactMemoUsed += uint64(len(node.children) * CHILDMAPBYTE)
	}
	invertedIndex := node.invertedIndex
	if len(invertedIndex) > 0 {
		for _, v := range invertedIndex {
			ExactMemoUsed += uint64(SIDBYTE + len(v)*POSBYTE)
		}
	}
	addrOffset := node.addrOffset
	if len(addrOffset) > 0 {
		ExactMemoUsed += uint64(len(addrOffset) * (ADDRBYTE + OFFSETBYTE))
	}
	for _, child := range node.children {
		child.GetMemorySizeOfIndexTreeExact(level + 1)
	}
}

//Calculate the length of each gram
var Tokens [][]string
var temp []string
var SumInvertLen = 0

func (root *IndexTreeNode) SearchGramsFromIndexTree() {
	if len(root.children) == 0 {
		return
	}
	for _, child := range root.children {
		temp = append(temp, child.data)
		if child.isleaf == true {
			for j := 0; j < len(temp); j++ {
				val := temp[j]
				SumInvertLen += len(val)
			}
			Tokens = append(Tokens, temp)
		}
		child.SearchGramsFromIndexTree()
		if len(temp) > 0 {
			temp = temp[:len(temp)-1]
		}
	}
}
