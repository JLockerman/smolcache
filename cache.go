package smolcache

import (
	"sync"
	"sync/atomic"

	"hash/maphash"
)

// CLOCK based approximate LRU storing mappings from strings to
// int64s designed for concurrent usage.
// It is sharded into 127 blocks, each of which can be
// locked independently. Gets only require a read lock on the
// individual block the element can be found in. When the cache is
// not full, Insert only requires a write lock on the individual
// block. Eviction locks blocks one at a time looking for a value
// that's valid to evict/
type Interner struct {
	maps [127]block

	max  uint64
	seed maphash.Seed

	// padding so that the count, which changes frequently, doesn't
	// share a cache line with the max and seed, which are read only
	_padding [48]byte

	count uint64

	clockLock sync.Mutex

	// CLOCK sweep state, guarded by clockLock
	clock uint8
}

type block struct {
	lock sync.RWMutex
	// guarded by lock, indexes into elements
	mapping map[string]int

	elements []Element

	//index into elements
	first_free_element int

	// CLOCK sweep state, guarded by clockLock
	next int //index into elements
	// pad blocks out to be cache aligned
	_padding [56]byte
}

type Element struct {
	// The value stored with this element.
	key   string
	Value int64

	// index of this element in elements
	idx int

	// when this is on the freelist, the index of the next free element
	next_free int

	//CLOCK marker if this is recently used
	used uint32

	_padding [16]uint8
}

func WithMax(max uint64) *Interner {
	return &Interner{
		max:  max,
		seed: maphash.MakeSeed(),
		maps: [127]block{block{first_free_element: -1}},
	}
}

func (i *Interner) Insert(key string, value int64) {
	newSize := atomic.AddUint64(&i.count, 1)
	needsEvict := newSize > i.max
	if needsEvict {
		i.evict()
	}

	h := maphash.Hash{}
	h.SetSeed(i.seed)
	h.WriteString(key)
	blockNum := h.Sum64() % 127
	block := &i.maps[blockNum]
	block.insert(key, value)
}

func (b *block) insert(key string, value int64) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.mapping == nil {
		b.mapping = make(map[string]int)
	}
	_, present := b.mapping[key]
	if present {
		return false
	}
	var idx int
	if b.first_free_element > 0 {
		element := &b.elements[b.first_free_element]
		b.first_free_element = element.next_free
		element.next_free = -1
		element.key = key
		element.Value = value
		element.used = 0
		idx = element.idx
	} else {
		idx = len(b.elements)
		b.elements = append(b.elements, Element{
			key:   key,
			Value: value,
			idx:   idx,
		})
	}
	b.mapping[key] = idx
	return true
}

func (i *Interner) evict() {
	i.clockLock.Lock()
	defer i.clockLock.Unlock()
	if i.count == 0 {
		return
	}
	for {
		block := &i.maps[i.clock%127]
		evicted, reachedEnd := block.tryEvict()
		if reachedEnd {
			i.clock += 1
		}
		if evicted {
			atomic.AddUint64(&i.count, ^uint64(0))
			break
		}
	}
}

func (b *block) tryEvict() (evicted bool, reachedEnd bool) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.next == len(b.elements) {
		b.next = 0
		if b.next == len(b.elements) {
			return false, true
		}
	}

	evicted = false
	reachedEnd = false
	for !evicted && !reachedEnd {
		idx := b.next
		b.next = idx + 1
		reachedEnd = b.next == len(b.elements)
		elem := &b.elements[idx]
		if elem.used == 1 {
			elem.used = 0
		} else if elem.used == 0 {
			elem.used = 2
			delete(b.mapping, elem.key)
			elem.Value = 0
			elem.next_free = b.first_free_element
			b.first_free_element = idx
			evicted = true
		}
	}

	return evicted, reachedEnd
}

func (i *Interner) Get(key string) (int64, bool) {
	h := maphash.Hash{}
	h.SetSeed(i.seed)
	h.WriteString(key)
	blockNum := h.Sum64() % 127
	block := &i.maps[blockNum]
	return block.get(key)
}

func (b *block) get(key string) (int64, bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	if b.mapping == nil {
		return 0, false
	}
	idx, present := b.mapping[key]
	if !present {
		return 0, false
	}
	elem := &b.elements[idx]

	if atomic.LoadUint32(&elem.used) == 0 {
		atomic.StoreUint32(&elem.used, 1)
	}

	return elem.Value, true
}

func (i *Interner) Unmark(key string) bool {
	h := maphash.Hash{}
	h.SetSeed(i.seed)
	h.WriteString(key)
	blockNum := h.Sum64() % 127
	block := &i.maps[blockNum]
	return block.unmark(key)
}

func (b *block) unmark(key string) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()
	if b.mapping == nil {
		return false
	}
	idx, present := b.mapping[key]
	if !present {
		return false
	}
	elem := &b.elements[idx]

	if atomic.LoadUint32(&elem.used) != 0 {
		atomic.StoreUint32(&elem.used, 0)
	}

	return true
}

func (i *Interner) Len() uint64 {
	return atomic.LoadUint64(&i.count)
}
