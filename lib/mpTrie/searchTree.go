package mpTrie

import (
	"github.com/openGemini/openGemini/lib/utils"
	"os"
	"strings"
)

type SearchTree struct {
	root *SearchTreeNode
}

func (tree *SearchTree) Root() *SearchTreeNode {
	return tree.root
}

func NewSearchTree() *SearchTree {
	return &SearchTree{root: NewSearchTreeNode("")}
}


func (tree *SearchTree) InsertGramForMulti(addrcache *AddrCache, invtdcache *InvertedCache, fileid int,file *os.File, data string, obj *SerializeObj) {
	root := tree.root
	for i, c := range data {
		cur := int(c)
		if root.children[cur] == nil {
			root.children[cur] = NewSearchTreeNode(data[i : i+1])
		}
		root = root.children[cur]
		root.data = data[i : i+1]
	}
	root.freq = int(obj.Freq())
	//Add addrmap addrInfo
	addrlen := int(obj.AddrListlen())
	addrmp := root.AddrCheck()
	if addrlen != 0 {
		off := obj.AddrListEntry().Blockoffset()
		addrInfo := UnserializeAddrListBlk(off, file)
		addrcache.Put(off,fileid, addrInfo)
		addrblksize := obj.AddrListEntry().Size()
		addrmp[fileid] = NewAddrInfo(addrlen,off,addrblksize)
	}
	//Add invtdmap invtdInfo
	invtdmp := root.InvtdCheck()
	invtdlen := int(obj.InvertedListlen())
	if invtdlen != 0 {
		off := obj.InvertedListEntry().Blockoffset()
		invtdInfo := UnserializeInvertedListBlk(off, file)
		invtdcache.Put(off,fileid, invtdInfo)
		ivtdblksize := obj.InvertedListEntry().Size()
		invtdmp[fileid] = NewInvtdInfo(invtdlen,off,ivtdblksize)
	}
	root.isleaf = true
}
func (tree *SearchTree) SearchPrefix(prefix string) *SearchTreeNode {
	root := tree.root
	for _, c := range prefix {
		cur := int(c)
		if root.children[cur] == nil {
			root.children[cur] = &SearchTreeNode{}
		}
		root = root.children[cur]
	}
	return root
}

func (tree *SearchTree) Search(data string) bool {
	node := tree.SearchPrefix(data)
	return node.isleaf && node != nil
}

func (tree *SearchTree) PrintSearchTree(file map[int]*os.File, addrcache *AddrCache, invtdcache *InvertedCache) {
	tree.root.printsearchTreeNode(file,0, addrcache, invtdcache)
}


func (tree *SearchTree) InsertTokenForMulti(addrcache *AddrCache, invtdcache *InvertedCache, fileid int,file *os.File, data string, obj *SerializeObj) {
	root := tree.root
	tokens := strings.Split(data, " ")
	for _, token := range tokens {
		cur := utils.StringToHashCode(token)
		if root.children[cur] == nil {
			root.children[cur] = NewSearchTreeNode(token)
		}
		root = root.children[cur]
		root.data = token
	}
	root.freq = int(obj.Freq())
	//Add addrmap addrInfo
	addrlen := int(obj.AddrListlen())
	addrmp := root.AddrCheck()
	if addrlen != 0 {
		off := obj.AddrListEntry().Blockoffset()
		addrInfo := UnserializeAddrListBlk(off, file)
		addrcache.Put(off,fileid, addrInfo)
		addrblksize := obj.AddrListEntry().Size()
		addrmp[fileid] = NewAddrInfo(addrlen,off,addrblksize)
	}
	//Add invtdmap invtdInfo
	invtdmp := root.InvtdCheck()
	invtdlen := int(obj.InvertedListlen())
	if invtdlen != 0 {
		off := obj.InvertedListEntry().Blockoffset()
		invtdInfo := UnserializeInvertedListBlk(off, file)
		invtdcache.Put(off,fileid, invtdInfo)
		ivtdblksize := obj.InvertedListEntry().Size()
		invtdmp[fileid] = NewInvtdInfo(invtdlen,off,ivtdblksize)
	}

	root.isleaf = true
}

func (tree *SearchTree) SearchTokenPrefix(prefix string) *SearchTreeNode {
	root := tree.root
	tokens := strings.Split(prefix, " ")
	for _, token := range tokens {
		cur := utils.StringToHashCode(token)
		if root.children[cur] == nil {
			root.children[cur] = &SearchTreeNode{}
		}
		root = root.children[cur]
	}
	return root
}

func (tree *SearchTree) SearchToken(data string) bool {
	node := tree.SearchTokenPrefix(data)
	return node.isleaf && node != nil
}

