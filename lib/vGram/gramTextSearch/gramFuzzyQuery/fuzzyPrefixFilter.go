package gramFuzzyQuery

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramMatchQuery"
	"os"
	"sort"
	"time"
)
func AbsInt(a int8) int8 {
	if a >= 0 {
		return a
	} else {
		return -a
	}
}
func minDistanceToken(word1 string, word2 string) int {
	l1 := len(word1)
	l2 := len(word2)

	dp := make([][]int, l1+1)

	dp[0] = make([]int, l2+1)
	for j := 0; j <= l2; j++ {
		dp[0][j] = j
	}
	for i := 1; i <= l1; i++ {
		dp[i] = make([]int, l2+1)
		dp[i][0] = i
		for j := 1; j <= l2; j++ {
			if word1[i-1:i] == word2[j-1:j] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = MinThree(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]) + 1
			}
		}
	}
	return dp[l1][l2]
}

func VerifyED(searStr string, dataStr string, distance int) bool {
	if minDistanceToken(searStr, dataStr) <= distance {
		return true
	} else {
		return false
	}
}
func FuzzyQueryPrefixIndex(shortIndex map[int]map[string]struct{},longIndex map[string]map[int]map[utils.FuzzyPrefixGram]struct{},searchStr string, dicRootNode *gramClvc.TrieTreeNode, indexRootNode *mpTrie.SearchTreeNode,  fileId int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache,qmin int, qmax int, distance int,prefixLen int,resMapFuzzy map[utils.SeriesId]struct{})  {
	sum:=0
	sumPass:=0
	if len(searchStr) > qmax+distance {
		fmt.Println("error:查询语句长度大于qmax+distance,无法匹配结果")
		return
	}

	lenSearchStr := len(searchStr)
	if lenSearchStr<prefixLen*distance+1+prefixLen-1{
		for key,value:=range shortIndex{
			if key > lenSearchStr+distance || key < lenSearchStr-distance {
				continue
			}else{
				for str:=range value{
					sum++
					verifyresult := VerifyED(searchStr, str, distance)
					if verifyresult {
						sumPass++
						var vgMap = make(map[uint16]string)
						gramIndex.VGConsBasicIndex(dicRootNode, qmin, str, vgMap)
						if len(vgMap) == 1 {
							resMapFuzzy = FuzzyReadInver(resMapFuzzy, vgMap, indexRootNode, fileId, filePtr, addrCache, invertedCache)
						} else {
							gramMatchQuery.MatchSearch2(vgMap, indexRootNode, fileId, filePtr, addrCache, invertedCache, resMapFuzzy)
						}
					}
				}
				continue
			}
		}

		indexInver:=make(map[string]struct{})
		for _,preIndex:=range longIndex{
			for lenData,indexData:=range preIndex {
				if lenData>lenSearchStr+distance||lenData<lenSearchStr-distance {
					continue
				}else {
					for data:=range indexData{
						indexInver[data.Gram()]= struct{}{}
					}
				}
			}

		}
		for key, _ := range indexInver {
			sum++
			verifyresult3 := VerifyED(searchStr, key, distance)
			if verifyresult3 {
				sumPass++
				var vgMap = make(map[uint16]string)
				gramIndex.VGConsBasicIndex(dicRootNode, qmin, key, vgMap)
				if len(vgMap) == 1 {
					resMapFuzzy = FuzzyReadInver(resMapFuzzy, vgMap, indexRootNode, fileId, filePtr, addrCache, invertedCache)
				} else {
					gramMatchQuery.MatchSearch2(vgMap, indexRootNode, fileId, filePtr, addrCache, invertedCache, resMapFuzzy)
				}
			}
		}
	}else{
		var qgramSearch = make([]utils.FuzzyPrefixGram, 0)
		for i := 0; i < lenSearchStr-prefixLen+1; i++ {
			qgramSearch = append(qgramSearch, utils.NewFuzzyPrefixGram(searchStr[i:i+prefixLen], int8(i)))
		}
		sort.SliceStable(qgramSearch, func(i, j int) bool {
			if qgramSearch[i].Gram() < qgramSearch[j].Gram() {
				return true
			}
			return false
		})
		prefixgramcount := prefixLen*distance + 1

		var mapsearchGram = make(map[string][]int8)
		if lenSearchStr-prefixLen+1 >= prefixgramcount {
			for i := 0; i < prefixgramcount; i++ {
				mapsearchGram[qgramSearch[i].Gram()] = append(mapsearchGram[qgramSearch[i].Gram()], qgramSearch[i].Pos())
			}
		}

		for key,value:=range shortIndex{
			if key > lenSearchStr+distance || key < lenSearchStr-distance {
				continue
			}else{
				for str:=range value{
					sum++
					verifyresult := VerifyED(searchStr, str, distance)
					if verifyresult {
						sumPass++
						var vgMap = make(map[uint16]string)
						gramIndex.VGConsBasicIndex(dicRootNode, qmin, str, vgMap)
						if len(vgMap) == 1 {
							resMapFuzzy = FuzzyReadInver(resMapFuzzy, vgMap, indexRootNode, fileId, filePtr, addrCache, invertedCache)
						} else {
							gramMatchQuery.MatchSearch2(vgMap, indexRootNode, fileId, filePtr, addrCache, invertedCache, resMapFuzzy)
						}
					}
				}
				continue
			}
		}

		indexInver:=make(map[string]struct{})
		for key,value:=range mapsearchGram{
			for index,preIndex:=range longIndex[key]{
				if index>lenSearchStr+distance||index<lenSearchStr-distance{
					continue
				}else{
					for mapPre:=range preIndex{
						for n := 0; n < len(value); n++ {
							if AbsInt(value[n]-mapPre.Pos()) <= int8(distance) {
								indexInver[mapPre.Gram()]= struct{}{}
								break
							}
						}
					}

				}
			}
		}
		for key, _ := range indexInver {
			sum++
			verifyresult2 := VerifyED(searchStr, key, distance)
			if verifyresult2 {
				sumPass++
				var vgMap = make(map[uint16]string)
				gramIndex.VGConsBasicIndex(dicRootNode, qmin, key, vgMap)
				if len(vgMap) == 1 {
					resMapFuzzy = FuzzyReadInver(resMapFuzzy, vgMap, indexRootNode, fileId, filePtr, addrCache, invertedCache)
				} else {
					gramMatchQuery.MatchSearch2(vgMap, indexRootNode, fileId, filePtr, addrCache, invertedCache, resMapFuzzy)
				}
			}
		}
	}

	return
}
func FuzzyQueryPrefixFilterTries(shortFuzzyIndex map[int]map[string]struct{},longFuzzyIndex map[string]map[int]map[utils.FuzzyPrefixGram]struct{}, searchStr string, root *gramClvc.TrieTreeNode, indexRoots *mpTrie.SearchTreeNode, qmin int, qmax int, distance int,prefixlen int, filePtr map[int]*os.File, addrCache *mpTrie.AddrCache, invertedCache *mpTrie.InvertedCache) map[utils.SeriesId]struct{} {
	start := time.Now().UnixMicro()
	var resArr = make(map[utils.SeriesId]struct{})
	for fileId, _ := range filePtr {
		FuzzyQueryPrefixIndex(shortFuzzyIndex,longFuzzyIndex,searchStr,root,indexRoots,fileId,filePtr,addrCache,invertedCache,qmin,qmax,distance,prefixlen,resArr)
	}
	end := time.Now().UnixMicro()
	fmt.Println("近似查询时间:(ms)", float64(end-start)/1000)
	fmt.Println("近似结果条数:", len(resArr))
	return resArr
}

