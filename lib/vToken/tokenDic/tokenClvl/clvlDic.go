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

package tokenClvl

import (
	"sort"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
)

type GramDictionary interface {
	GenerateClvlDictionaryTree(logs map[utils.SeriesId]string, qmin int, sampleStringOfW map[utils.SeriesId]string)
}

type CLVLDic struct {
	TrieTree *tokenClvc.TrieTree
}

func NewCLVLDic(qmin int, qmax int) *CLVLDic {
	return &CLVLDic{
		TrieTree: tokenClvc.NewTrieTree(qmin, qmax),
	}
}

/*
	logs are log data, qmin are the minimum lengths for dictionary building and dividing the index items,
	and sampleStringOfW is the extracted query payload, the return value is a dictionary tree.
*/

func (clvlDic *CLVLDic) GenerateClvlDictionaryTree(logs map[utils.SeriesId]string, qmin int, sampleStringOfW map[utils.SeriesId]string) {
	tokenDic := GenerateQminIndexTree(logs, qmin)
	tokentriew, _ := GenTokenTw(tokenDic, sampleStringOfW, qmin)
	queuelist := LeafToQueue(tokenDic)
	queue := make([]*tokenIndex.IndexTreeNode, 0)
	for _, onenode := range queuelist {
		node := tokenIndex.NewIndexTreeNode(onenode.Data())
		node.SetIsleaf(true)
		node.SetInvertedIndex(onenode.InvertedIndex())
		queue = append(queue, node)
	}
	floorNum := qmin
	for len(queue) != 0 {
		var newqueue []*tokenIndex.IndexTreeNode
		for _, data := range queue {
			indexlist := data.InvertedIndex()
			var sidSort []utils.SeriesId
			for oneSid, _ := range indexlist {
				sidSort = append(sidSort, oneSid)
			}
			sort.SliceStable(sidSort, func(i, j int) bool {
				if sidSort[i].Id < sidSort[j].Id {
					return true
				}
				return false
			})
			for sidindex := 0; sidindex < len(sidSort); sidindex++ {
				oneSid := sidSort[sidindex]
				oneArray := indexlist[oneSid]
				nextgram := make(map[string]utils.Inverted_index, 0)
				keys := make([]string, 0)
				var evaluateResult bool
				var cNode, suffixNode *tokenIndex.IndexTreeNode
				var pathsuffix []string
				var commomsuffix []string
				curstr := logs[oneSid]
				curfields, _ := utils.DataProcess(curstr)
				poslist := oneArray
				if len(poslist) == 0 && poslist == nil {
					break
				} else {
					if len(poslist) >= 1 {
						for i := 0; i < len(poslist); i++ {
							pos := poslist[i] + uint16(floorNum)
							if int(pos) < len(curfields) {
								for m := poslist[i]; m < pos; m++ {
									pathsuffix = append(pathsuffix, curfields[m])
								}
								for n := poslist[i] + 1; n < pos; n++ {
									commomsuffix = append(commomsuffix, curfields[n])
								}
								curNextToken := curfields[pos]
								if nextgram[curNextToken] == nil {
									nextgram[curNextToken] = make(map[utils.SeriesId][]uint16)
								}
								nextgram[curNextToken][oneSid] = append(nextgram[curNextToken][oneSid], poslist[i])
								keys = append(keys, curNextToken)
							}
						}
					}
					for key := 0; key < len(keys); key++ {
						nexttoken := keys[key]
						trieNode := GetSuffixTokenNode(tokenDic, pathsuffix)
						if trieNode == nil || len(trieNode.InvertedIndex()) == 0 {
							continue
						}
						suffixstr := append(commomsuffix, nexttoken)
						suffixstr, suffixNode = GetloggestSuffixTokenNode(tokenDic, suffixstr, qmin)
						if suffixNode == nil || len(suffixstr) == 0 {
							suffixNode = tokenIndex.NewIndexTreeNode("")
						}
						//cnode,扩展gram的倒排链表
						cNode = tokenIndex.NewIndexTreeNode(nexttoken)
						cNode.SetInvertedIndex(nextgram[nexttoken])
						cNode.SetIsleaf(true)
						allstr := append(pathsuffix, nexttoken)
						evaluateResult = EvaluateToken(len(sampleStringOfW), trieNode, pathsuffix, cNode, allstr, suffixNode, suffixstr, tokentriew, sampleStringOfW)
						if evaluateResult {
							for cdelSid, cdelPosArray := range cNode.InvertedIndex() {
								//修改本地未扩展之前对应gram的倒排列表
								if len(trieNode.InvertedIndex()) > 0 {
									RemoveInvertedIndex(trieNode.InvertedIndex(), cdelSid, cdelPosArray)
								}
								//修改最长后缀的倒排列表
								if len(suffixNode.InvertedIndex()) > 0 {
									SuffixRemoveInvertedIndex(suffixNode.InvertedIndex(), cdelSid, cdelPosArray)
								}
							}
							InsertIndexNode(trieNode, cNode)
							newqueue = append(newqueue, cNode)
						} else {
							continue
						}
					}

				}
			}

		}
		floorNum++
		queue = newqueue
	}
	tokenDic.SetQmax(MaxDepth(tokenDic))
	clvlDic.TrieTree = TrieNodeTrans(tokenDic)
}
