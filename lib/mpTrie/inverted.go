package mpTrie

//LRU
type InvertedCache struct {
	capicity int
	used     int
	blkcache map[uint64]*InvertedNode
	head     *InvertedNode
	tail     *InvertedNode
}

func (this *InvertedCache) Capicity() int {
	return this.capicity
}

func (this *InvertedCache) SetCapicity(capicity int) {
	this.capicity = capicity
}

func (this *InvertedCache) Used() int {
	return this.used
}

func (this *InvertedCache) SetUsed(used int) {
	this.used = used
}

func (this *InvertedCache) Blkcache() map[uint64]*InvertedNode {
	return this.blkcache
}

func (this *InvertedCache) SetBlkcache(blkcache map[uint64]*InvertedNode) {
	this.blkcache = blkcache
}

func (this *InvertedCache) Head() *InvertedNode {
	return this.head
}

func (this *InvertedCache) SetHead(head *InvertedNode) {
	this.head = head
}

func (this *InvertedCache) Tail() *InvertedNode {
	return this.tail
}

func (this *InvertedCache) SetTail(tail *InvertedNode) {
	this.tail = tail
}

type InvertedNode struct {
	key   uint64 //offset
	value *InvertedListBlock
	prev  *InvertedNode
	next  *InvertedNode
}

func NewEntry(key uint64, value *InvertedListBlock) *InvertedNode {
	return &InvertedNode{key: key, value: value}
}

func InitInvertedCache(capicity int) *InvertedCache {
	cache := &InvertedCache{
		capicity: capicity,
		used:     0,
		blkcache: make(map[uint64]*InvertedNode),
		head:     NewEntry(0, nil),
		tail:     NewEntry(0, nil),
	}
	cache.head.next = cache.tail
	cache.tail.prev = cache.head
	return cache
}

func (this *InvertedCache) Put(offset uint64, blk *InvertedListBlock) {
	_, ok := this.blkcache[offset]
	if ok {
		entry := this.blkcache[offset]
		entry.value = blk
		//delete and move to head
		this.DeleteEntry(entry)
		this.AddEntryToHead(entry)
	} else {
		tmp := NewEntry(offset, blk)
		this.blkcache[offset] = tmp
		this.AddEntryToHead(tmp)
		this.AddSize()
		for this.used > this.capicity {
			real := this.DeleteTail()
			delete(this.blkcache, real.key)
			this.DecSize()
		}
	}

}

func (this *InvertedCache) Get(offset uint64) *InvertedListBlock {
	if blk, ok := this.blkcache[offset]; ok {
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
