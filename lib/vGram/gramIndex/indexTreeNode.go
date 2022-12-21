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
package gramIndex

import (
	"fmt"
	"unsafe"

	"github.com/openGemini/openGemini/lib/utils"
)

type IndexTreeNode struct {
	data          string
	frequency     int
	children      map[uint8]*IndexTreeNode
	isleaf        bool
	invertedIndex utils.Inverted_index
	addrOffset    map[*IndexTreeNode]uint16
}

func (node *IndexTreeNode) Frequency() int {
	return node.frequency
}

func (node *IndexTreeNode) SetFrequency(frequency int) {
	node.frequency = frequency
}

func (node *IndexTreeNode) AddrOffset() map[*IndexTreeNode]uint16 {
	return node.addrOffset
}

func (node *IndexTreeNode) SetAddrOffset(addrOffset map[*IndexTreeNode]uint16) {
	node.addrOffset = addrOffset
}

func (node *IndexTreeNode) Data() string {
	return node.data
}

func (node *IndexTreeNode) SetData(data string) {
	node.data = data
}

func (node *IndexTreeNode) Children() map[uint8]*IndexTreeNode {
	return node.children
}

func (node *IndexTreeNode) SetChildren(children map[uint8]*IndexTreeNode) {
	node.children = children
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

func NewIndexTreeNode(data string) *IndexTreeNode {
	return &IndexTreeNode{
		data:          data,
		frequency:     1,
		isleaf:        false,
		children:      make(map[uint8]*IndexTreeNode),
		invertedIndex: make(map[utils.SeriesId][]uint16),
		addrOffset:    make(map[*IndexTreeNode]uint16),
	}
}

// Determine whether children have this node
func GetIndexNode(children map[uint8]*IndexTreeNode, char uint8) int8 {
	if children[char] != nil {
		return int8(char)
	}
	return -1
}

func (node *IndexTreeNode) InsertPosArrToInvertedIndexMap(sid utils.SeriesId, position uint16) {
	//Find the invertedIndex of sid in the inverted listArray, and add position to the posArray in the invertedIndex
	node.invertedIndex[sid] = append(node.invertedIndex[sid], position)
}

// Insert a new inverted structure
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
	//fmt.Print(node.data, " - ", node.frequency, " - ", node.isleaf, " - ", node.invertedIndex, " - ", node.addrOffset)
	fmt.Print(node.data, " - ", node.frequency, " - ", node.isleaf, " - ", len(node.invertedIndex), " - ", len(node.addrOffset))
	for _, child := range node.children {
		child.PrintIndexTreeNode(level + 1)
	}
}

var TheoreticalMemoUsed uint64 = 0
var ExactMemoUsed uint64 = 0

const NODEDATABYTE = 1
const NODEISLEAFBYTE = 1
const FREQUENCY = 4

const CHILDMAPBYTE = 1

const SIDBYTE = 16
const POSBYTE = 2

const ADDRBYTE = 1
const OFFSETBYTE = 2

var InvertedSize uint64 = 0
var AddrSize uint64 = 0

func (node *IndexTreeNode) GetMemorySizeOfIndexTreeTheoretical(level int) { //unsafe.sizeof
	TheoreticalMemoUsed += (NODEISLEAFBYTE + NODEDATABYTE + FREQUENCY)
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
var Grams []string
var temp string
var SumInvertLen = 0

func (node *IndexTreeNode) SearchGramsFromIndexTree() {
	if len(node.children) == 0 {
		return
	}
	for _, child := range node.children {
		if child != nil {
			temp += child.data
			if child.isleaf == true {
				SumInvertLen += len(temp)
				Grams = append(Grams, temp)
			}
			child.SearchGramsFromIndexTree()
			if len(temp) > 0 {
				temp = temp[0 : len(temp)-1]
			}
		}
	}
}

// Calculate the length of each invertedList
var Res []int
var Rea []int

func (node *IndexTreeNode) FixInvertedIndexSize() {
	for _, child := range node.children {
		if child.isleaf == true && len(child.invertedIndex) > 0 {
			Res = append(Res, len(child.invertedIndex)) //The append function must be used, and i cannot be used for variable addition, because there is no make initialization
		}
		child.FixInvertedIndexSize()
	}
}

func (node *IndexTreeNode) FixInvertedAddrSize() {
	for _, child := range node.children {
		if child.isleaf == true && len(child.addrOffset) > 0 {
			Rea = append(Rea, len(child.addrOffset)) //The append function must be used, and i cannot be used for variable addition, because there is no make initialization
		}
		child.FixInvertedAddrSize()
	}
}