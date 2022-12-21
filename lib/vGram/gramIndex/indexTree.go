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
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
)

type IndexTree struct {
	qmin int
	qmax int
	cout int
	root *IndexTreeNode
}

func (tree *IndexTree) Qmin() int {
	return tree.qmin
}

func (tree *IndexTree) SetQmin(qmin int) {
	tree.qmin = qmin
}

func (tree *IndexTree) Qmax() int {
	return tree.qmax
}

func (tree *IndexTree) SetQmax(qmax int) {
	tree.qmax = qmax
}

func (tree *IndexTree) Cout() int {
	return tree.cout
}

func (tree *IndexTree) SetCout(cout int) {
	tree.cout = cout
}

func (tree *IndexTree) Root() *IndexTreeNode {
	return tree.root
}

func (tree *IndexTree) SetRoot(root *IndexTreeNode) {
	tree.root = root
}

func NewIndexTree(qmin int, qmax int) *IndexTree {
	return &IndexTree{
		qmin: qmin,
		qmax: qmax,
		cout: 0,
		root: NewIndexTreeNode(""),
	}
}

// 08
func NewIndexTrie(qmin int) *IndexTree {
	return &IndexTree{
		qmin: qmin,
		cout: 0,
		root: NewIndexTreeNode(""),
	}
}

// Insert gram into IndexTree  position:The starting position of the strat in the statement
func (tree *IndexTree) InsertIntoIndexTree(gram string, inverted_index utils.Inverted_index) *IndexTreeNode {
	node := tree.root
	var addr *IndexTreeNode
	var childIndex int8 = -1
	for i := 0; i < len(gram); i++ {
		childIndex = GetIndexNode(node.children, gram[i])
		if childIndex == -1 {
			currentNode := NewIndexTreeNode(string(gram[i]))
			node.children[gram[i]] = currentNode
			node = currentNode
		} else {
			node = node.children[uint8(childIndex)]
			node.frequency++
		}
		if i == len(gram)-1 { //Leaf node, need to hook up linkedList
			node.isleaf = true
			node.invertedIndex = inverted_index
			addr = node
		}
	}
	return addr
}

func (tree *IndexTree) InsertOnlyGramIntoIndexTree(gramSubs []SubGramOffset, addr *IndexTreeNode) {
	var childIndex int8 = -1
	for k := 0; k < len(gramSubs); k++ {
		gram := gramSubs[k].subGram
		offset := gramSubs[k].offset
		node := tree.root
		for i := 0; i < len(gram); i++ {
			childIndex = GetIndexNode(node.children, gram[i])
			if childIndex == -1 {
				currentNode := NewIndexTreeNode(string(gram[i]))
				node.children[gram[i]] = currentNode
				node = currentNode
			} else {
				node = node.children[uint8(childIndex)]
				node.frequency++
			}
			if i == len(gram)-1 { //Leaf node, need to hook up linkedList
				node.isleaf = true
				if _, ok := node.addrOffset[addr]; !ok {
					node.addrOffset[addr] = offset
				}
			}
		}
	}
}

// 08
func (tree *IndexTree) InsertStringIntoIndexTree(gram string) {
	node := tree.root
	qmin := tree.qmin
	var childIndex int8 = -1 // The position of the child node in the ChildrenMap
	for i := 0; i < len(gram); i++ {
		childIndex = GetIndexNode(node.children, gram[i])
		if childIndex == -1 { // There is no such node in the ChildrenMap
			currentNode := NewIndexTreeNode(string(gram[i]))
			node.children[gram[i]] = currentNode
			node = currentNode
		} else { //There is this node in the ChildrenMap, so childrenIndex is the position of the node in the ChildrenMap
			node = node.children[uint8(childIndex)]
			node.frequency++
		}
		if i >= qmin-1 { //As long as the gram length is greater than qmin - 1, it is a leaf node
			node.isleaf = true
		}
	}
}

func (tree *IndexTree) PrintIndexTree() {
	tree.root.PrintIndexTreeNode(0)
}

// regexTestCLVL need
func (indextree *IndexTree) ToDicTree() *gramClvc.TrieTree {
	r := indextree.root.ConvertNode()
	trietree := gramClvc.NewTrieTree(indextree.qmin, indextree.qmax)
	trietree.SetRoot(r)
	return trietree
}

// regexTestCLVL need
func (indextree *IndexTreeNode) ConvertNode() *gramClvc.TrieTreeNode {
	node := gramClvc.NewTrieTreeNode(indextree.data)
	node.SetIsleaf(indextree.isleaf)
	for i := range indextree.children {
		ctrienode := indextree.children[i].ConvertNode()
		node.Children()[i] = ctrienode
	}
	return node
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
	fmt.Println("==================INVERTEDSIZE===================")
	fmt.Println(InvertedSize)
	fmt.Println("==================ADDRSIZE=======================")
	fmt.Println(AddrSize)
}

func (tree *IndexTree) SearchTermLengthAndTermAvgLenFromIndexTree() {
	tree.Root().SearchGramsFromIndexTree()
	fmt.Println("============== grams len: =======================")
	fmt.Println(len(Grams))
	fmt.Println("============== grams agv: =======================")
	fmt.Println(SumInvertLen / len(Grams))
}
