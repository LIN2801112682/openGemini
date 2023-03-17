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
)

type IndexTree struct {
	qmin int
	qmax int
	cout int
	root *IndexTreeNode
}

func (i *IndexTree) Qmin() int {
	return i.qmin
}

func (i *IndexTree) SetQmin(qmin int) {
	i.qmin = qmin
}

func (i *IndexTree) Qmax() int {
	return i.qmax
}

func (i *IndexTree) SetQmax(qmax int) {
	i.qmax = qmax
}

func (i *IndexTree) Cout() int {
	return i.cout
}

func (i *IndexTree) SetCout(cout int) {
	i.cout = cout
}

func (i *IndexTree) Root() *IndexTreeNode {
	return i.root
}

func (i *IndexTree) SetRoot(root *IndexTreeNode) {
	i.root = root
}

// 初始化trieTree
func NewIndexTree(qmin int, qmax int) *IndexTree {
	return &IndexTree{
		qmin: qmin,
		qmax: qmax,
		cout: 0,
		root: NewIndexTreeNode(""),
	}
}

// 08年
func NewIndexTrie08(qmin int) *IndexTree {
	return &IndexTree{
		qmin: qmin,
		cout: 0,
		root: NewIndexTreeNode(""),
	}
}

func (tree *IndexTree) InsertIntoIndexTree(token []string, inverted_index utils.InvertedIndex, invert_index_len int) *IndexTreeNode {
	node := tree.root
	var childIndex = -1
	var addr *IndexTreeNode
	for i, str := range token {
		childIndex = GetIndexNode(node.children, token[i])
		if childIndex == -1 {
			currentNode := NewIndexTreeNode(str)
			node.children[utils.StringToHashCode(str)] = currentNode
			node = currentNode
			node.frequency = invert_index_len
		} else {
			node = node.children[childIndex]
			node.frequency += invert_index_len
		}
		if i == len(token)-1 {
			node.isleaf = true
			node.invertedIndex = inverted_index
			addr = node
		}
	}
	return addr
}

func (tree *IndexTree) InsertOnlyTokenIntoIndexTree(tokenSubs []SubTokenOffset, addr *IndexTreeNode, invert_index_len int) {
	var childIndex = -1
	for k := 0; k < len(tokenSubs); k++ {
		token := tokenSubs[k].subToken
		offset := tokenSubs[k].offset
		node := tree.root
		for i, str := range token {
			childIndex = GetIndexNode(node.children, token[i])
			if childIndex == -1 {
				currentNode := NewIndexTreeNode(str)
				node.children[utils.StringToHashCode(str)] = currentNode
				node = currentNode
				node.frequency = invert_index_len
			} else {
				node = node.children[childIndex]
				node.frequency += invert_index_len
			}
			if i == len(token)-1 {
				node.isleaf = true
				if _, ok := node.addrOffset[addr]; !ok {
					node.addrOffset[addr] = offset
				}
			}
		}
	}
}

/*func (tree *IndexTree) InsertTokensIntoIndexTree08(token *[]string, sid utils.SeriesId, position uint16) {
	node := tree.root
	var childindex = 0
	for i, str := range *token {
		childindex = GetIndexNode(node.children, (*token)[i])
		if childindex == -1 {
			currentnode := NewIndexTreeNode(str)
			node.children[utils.StringToHashCode(str)] = currentnode
			node = currentnode
		} else {
			node = node.children[childindex]
			node.frequency++
		}
		if i == len(*token)-1 {
			node.isleaf = true
			if _, ok := node.invertedIndex[sid]; !ok {
				node.InsertSidAndPosArrToInvertedIndexMap(sid, position)
			} else {
				node.InsertPosArrToInvertedIndexMap(sid, position)
			}
		}
	}
}*/

func (tree *IndexTree) PrintIndexTree() {
	tree.root.PrintIndexTreeNode(0)
}

func (tree *IndexTree) UpdateIndexRootFrequency() {
	for _, child := range tree.root.children {
		tree.root.frequency += child.frequency
	}
	tree.root.frequency--
}

func (tree *IndexTree) GetMemorySizeOfIndexTree() {
	tree.root.GetMemorySizeOfIndexTreeTheoretical(0)
	fmt.Println("==============Theoretical MemoUsed===============")
	fmt.Println(TheoreticalMemoUsed)
	tree.root.GetMemorySizeOfIndexTreeExact(0)
	fmt.Println("==============Exact MemoUsed=====================")
	fmt.Println(ExactMemoUsed)
	fmt.Println("==============INVERTEDSIZE=======================")
	fmt.Println(InvertedSize)
	fmt.Println("==================ADDRSIZE=======================")
	fmt.Println(AddrSize)
	fmt.Println("=====================NODES=======================")
	fmt.Println(Nodes)
	fmt.Println("==================POSITIONLIST===================")
	fmt.Println(PositionList)
}

func (root *IndexTree) SearchTermLengthAndTermAvgLenFromIndexTree() {
	root.Root().SearchGramsFromIndexTree()
	fmt.Println("============== tokens len: ======================")
	fmt.Println(len(Tokens))
	fmt.Println("============== tokens agv: ======================")
	fmt.Println(SumInvertLen / len(Tokens))
}
