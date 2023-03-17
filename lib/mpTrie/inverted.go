package mpTrie

//LRU
type InvertedCache struct {
	capicity int
	used     int
	blkcache map[uint32]*InvertedNode
	head     *InvertedNode
	tail     *InvertedNode
}

func (this *InvertedCache) Blkcache() map[uint32]*InvertedNode {
	return this.blkcache
}

func (this *InvertedCache) SetBlkcache(blkcache map[uint32]*InvertedNode) {
	this.blkcache = blkcache
}

type InvertedNode struct {
	key   uint32 //offset
	value *InvertedListBlock
	prev  *InvertedNode
	next  *InvertedNode
}

func NewEntry(key uint32, value *InvertedListBlock) *InvertedNode {
	return &InvertedNode{key: key, value: value}
}

func InitInvertedCache(capicity int) *InvertedCache {
	cache := &InvertedCache{
		capicity: capicity,
		used:     0,
		blkcache: make(map[uint32]*InvertedNode),
		head:     NewEntry(0, nil),
		tail:     NewEntry(0, nil),
	}
	cache.head.next = cache.tail
	cache.tail.prev = cache.head
	return cache
}

func (this *InvertedCache) Put(offset uint64,fileid int, blk *InvertedListBlock) {
	hash := getcacheHash(offset, fileid)
	_, ok := this.blkcache[hash]
	if ok {
		entry := this.blkcache[hash]
		entry.value = blk
		//delete and move to head
		this.DeleteEntry(entry)
		this.AddEntryToHead(entry)
	} else {
		tmp := NewEntry(hash, blk)
		this.blkcache[hash] = tmp
		this.AddEntryToHead(tmp)
		this.AddSize()
		for this.used > this.capicity {
			real := this.DeleteTail()
			delete(this.blkcache, real.key)
			this.DecSize()
		}
	}

}

func (this *InvertedCache) Get(offset uint64,fileid int) *InvertedListBlock {
	hash := getcacheHash(offset, fileid)
	if blk, ok := this.blkcache[hash]; ok {
		this.DeleteEntry(blk)
		this.AddEntryToHead(blk)
		return blk.value
	} else {
		return nil
	}
}

func (this *InvertedCache) DeleteTail() *InvertedNode {
	real := this.tail.prev
	this.DeleteEntry(real)
	return real
}

func (this *InvertedCache) DeleteEntry(entry *InvertedNode) {
	next := entry.next
	per := entry.prev
	per.next = next
	next.prev = per
}

func (this *InvertedCache) AddSize() {
	this.used++
}

func (this *InvertedCache) DecSize() {
	this.used--
}

func (this *InvertedCache) AddEntryToHead(entry *InvertedNode) {
	head := this.head
	entry.next = head.next
	entry.prev = head
	head.next.prev = entry
	head.next = entry

}
