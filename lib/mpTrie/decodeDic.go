package mpTrie

import (
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"strings"
)

func UnserializeGramDicFromFile(qmin int, qmax int, filename string) *gramClvc.TrieTree {
	buffer, _ := GetBytesFromFile(filename)
	bufstr := string(buffer)
	grams := strings.Split(bufstr, SPLITFLAG)
	grams = grams[:len(grams)-1]
	dictrie := gramClvc.NewTrieTree(qmin, qmax)
	for _, gram := range grams {
		dictrie.InsertIntoTrieTree(gram)
	}
	return dictrie
}

func UnserializeTokenDicFromFile(qmin int, qmax int, filename string) *tokenClvc.TrieTree {
	buffer, _ := GetBytesFromFile(filename)
	bufstr := string(buffer)
	res := strings.Split(bufstr, SPLITFLAG)
	res = res[:len(res)-1]
	dictrie := tokenClvc.NewTrieTree(qmin, qmax)
	for _, tokens := range res {
		token := strings.Split(tokens, " ")
		dictrie.InsertIntoTrieTree(&token)
	}
	return dictrie
}
