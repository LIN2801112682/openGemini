package mpTrie

import (
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"strings"
)

func DecodeLogTreeFromMultiFiles(files []string,qmax int) *gramIndex.LogTree{
	logtree := gramIndex.NewLogTree(qmax)
	for _,filename := range files{
		UnserializeLogTreeFromFile(filename,logtree)
	}
	return logtree
}


func UnserializeLogTreeFromFile(filename string,logtree *gramIndex.LogTree){
	buffer, _ := GetBytesFromFile(filename)
	bufstr := string(buffer)
	grams := strings.Split(bufstr, SPLITFLAG)
	grams = grams[:len(grams)-1]
	for _, gram := range grams {
		logtree.InsertIntoTrieTreeLogTree(gram)
	}
}

