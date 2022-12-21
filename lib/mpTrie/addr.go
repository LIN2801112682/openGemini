package mpTrie

type AddrNode struct {
	key   uint64
	value *AddrListBlock
	prev  *AddrNode
	next  *AddrNode
}

func NewAddrNode(key uint64, value *AddrListBlock) *AddrNode {
	return &AddrNode{key: key, value: value}
}

type AddrCache struct {
	capicity   int
	used       int
	blk        map[uint64]*AddrNode
	list       *AddrNode
	head, tail *AddrNode
}

func (this *AddrCache) Capicity() int {
	return this.capicity
}

func (this *AddrCache) SetCapicity(capicity int) {
	this.capicity = capicity
}

func (this *AddrCache) Used() int {
	return this.used
}

func (this *AddrCache) SetUsed(used int) {
	this.used = used
}

func (this *AddrCache) Blk() map[uint64]*AddrNode {
	return this.blk
}

func (this *AddrCache) SetBlk(blk map[uint64]*AddrNode) {
	this.blk = blk
}

func (this *AddrCache) List() *AddrNode {
	return this.list
}

func (this *AddrCache) SetList(list *AddrNode) {
	this.list = list
}

func (this *AddrCache) Head() *AddrNode {
	return this.head
}

func (this *AddrCache) SetHead(head *AddrNode) {
	this.head = head
}

func (this *AddrCache) Tail() *AddrNode {
	return this.tail
}

func (this *AddrCache) SetTail(tail *AddrNode) {
	this.tail = tail
}

func InitAddrCache(capicity int) *AddrCache {
	cache := &AddrCache{
		capicity: capicity,
		used:     0,
		blk:      make(map[uint64]*AddrNode),
		head:     NewAddrNode(0, nil),
		tail:     NewAddrNode(0, nil),
	}
	cache.head.next = cache.tail
	cache.tail.prev = cache.head
	return cache
}

func (this *AddrCache) Put(offset uint64, blk *AddrListBlock) {
	node, ok := this.blk[offset]
	if !ok {
		tmp := NewAddrNode(offset, blk)
		this.blk[offset] = tmp
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

func (this *AddrCache) Get(offset uint64) *AddrListBlock {
	if node, ok := this.blk[offset]; ok {
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
