package mpTrie

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"os"
	"strings"
)
func GetBytesFromFile(filename string) ([]byte, int64) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}
	filesize := fileinfo.Size()
	buffer := make([]byte, filesize)
	_, err = file.Read(buffer)
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}
	return buffer, filesize
}

func UnserializeGramDicFromFile(filename string, qmin,qmax int) *gramClvc.TrieTree {
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


func UnserializeTokenDicFromFile(filename string, qmin,qmax int) *tokenClvc.TrieTree {
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
