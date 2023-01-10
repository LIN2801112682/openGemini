package mpTrie

import (
	"hash/crc32"
)

type AddrNode struct {
	key   uint32
	value *AddrListBlock
	prev  *AddrNode
	next  *AddrNode
}

func NewAddrNode(key uint32, value *AddrListBlock) *AddrNode {
	return &AddrNode{key: key, value: value}
}

type AddrCache struct {
	capicity   int
	used       int
	blk        map[uint32]*AddrNode
	list       *AddrNode
	head, tail *AddrNode
}

func (this *AddrCache) Blk() map[uint32]*AddrNode {
	return this.blk
}

func (this *AddrCache) SetBlk(blk map[uint32]*AddrNode) {
	this.blk = blk
}

func InitAddrCache(capicity int) *AddrCache {
	cache := &AddrCache{
		capicity: capicity,
		used:     0,
		blk:      make(map[uint32]*AddrNode),
		head:     NewAddrNode(0, nil),
		tail:     NewAddrNode(0, nil),
	}
	cache.head.next = cache.tail
	cache.tail.prev = cache.head
	return cache
}

func getcacheHash(offset uint64, fileid int) uint32 {
	offbytes, _ := IntToBytes(int(offset), 8)
	fileIdbytes, _ := IntToBytes(fileid, 8)
	offbytes = append(offbytes, fileIdbytes...)
	hashcode := crc32.ChecksumIEEE(offbytes)
	return hashcode
} 
func (this *AddrCache) Put(offset uint64,fileid int, blk *AddrListBlock) {
	hash := getcacheHash(offset, fileid)
	node, ok := this.blk[hash]
	if !ok {
		tmp := NewAddrNode(hash, blk)
		this.blk[hash] = tmp
		this.AddToHead(tmp)
		this.UpSize()
		for this.used > this.capicity {
			real := this.DeleteTail()
			this.DeleteEntry(real)
			delete(this.blk, real.key)
			this.DownSize()
		}
	} else {
		node.value = blk
		this.DeleteEntry(node)
		this.AddToHead(node)
	}
}

func (this *AddrCache) Get(offset uint64,fileid int) *AddrListBlock {
	hash := getcacheHash(offset, fileid)
	if node, ok := this.blk[hash]; ok {
		this.DeleteEntry(node)
		this.AddToHead(node)
		return node.value
	} else {
		return nil
	}

}

func (this *AddrCache) UpSize() {
	this.used++
}
func (this *AddrCache) DownSize() {
	this.used--
}

func (this *AddrCache) DeleteEntry(entry *AddrNode) {
	entry.prev.next = entry.next
	entry.next.prev = entry.prev

}

func (this *AddrCache) DeleteTail() *AddrNode {
	realnode := this.tail.prev
	this.DeleteEntry(realnode)
	return realnode
}

func (this *AddrCache) AddToHead(node *AddrNode) {
	head := this.head
	node.next = head.next
	node.prev = head
	head.next.prev = node
	head.next = node
}
