package smolcache

import (
	"encoding/binary"
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
	// guarded by lock, stores indexes into storage
	elements map[interface{}]int

	storage []Element
	// CLOCK sweep state, index into storage guarded by clockLock
	next int
}

type Element struct {
	// The value stored with this element.
	key   interface{}
	Value interface{}

	//CLOCK marker if this is recently used
	used uint32

	// pad Elements out to be cache aligned
	_padding [24]byte
}

func WithMax(max uint64) *Interner {
	return &Interner{
		max:  max,
		seed: maphash.MakeSeed(),
	}
}

func WithMaxAndShards(max uint64, shards int) *Interner {
	//TODO variable number of shards
	return &Interner{
		max:  max,
		seed: maphash.MakeSeed(),
	}
}

func (i *Interner) GetSeed() maphash.Seed {
	return i.seed
}

func (i *Interner) InsertString(key string, value interface{}) {
	newSize := atomic.AddUint64(&i.count, 1)
	needsEvict := newSize > i.max
	if needsEvict {
		i.evict()
	}

	h := maphash.Hash{}
	h.SetSeed(i.seed)
	h.WriteString(key)
	i.InsertWithHash(key, value, h.Sum64())
}

func (i *Interner) InsertInt(key uint64, value interface{}) {
	newSize := atomic.AddUint64(&i.count, 1)
	needsEvict := newSize > i.max
	if needsEvict {
		i.evict()
	}

	h := maphash.Hash{}
	h.SetSeed(i.seed)
	b := [8]byte{}
	binary.LittleEndian.PutUint64(b[:], key)
	i.InsertWithHash(key, value, h.Sum64())
}

func (i *Interner) InsertWithHash(key interface{}, value interface{}, hash uint64) {
	blockNum := hash % 127
	block := &i.maps[blockNum]
	block.insert(key, value)
}

func (b *block) insert(key interface{}, value interface{}) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.elements == nil {
		b.elements = make(map[interface{}]int)
	}
	_, present := b.elements[key]
	if present {
		return false
	}
	index := len(b.storage)
	b.storage = append(b.storage, Element{key: key, Value: value})
	b.elements[key] = index
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
	if len(b.storage) == 0 {
		return false, true
	}
	if b.next >= len(b.storage) {
		b.next = 0
	}

	evicted = false
	reachedEnd = false
	for !evicted && !reachedEnd {
		elem := &b.storage[b.next]
		if elem.used != 0 {
			elem.used = 0
		} else {
			key := elem.key
			delete(b.elements, key)
			// if elem isn't the last element in storage, swap it
			// with the last element
			if b.next < len(b.storage)-1 {
				*elem = b.storage[len(b.storage)-1]
				b.elements[elem.key] = b.next
			}
			// pop off the last element
			b.storage[len(b.storage)-1] = Element{}
			b.storage = b.storage[:len(b.storage)-1]
			evicted = true
		}
		b.next += 1
		reachedEnd = b.next >= len(b.storage)
	}

	return evicted, reachedEnd
}

func (i *Interner) GetString(key string) (interface{}, bool) {
	h := maphash.Hash{}
	h.SetSeed(i.seed)
	h.WriteString(key)
	return i.GetWithHash(key, h.Sum64())
}

func (i *Interner) GetInt(key uint64) (interface{}, bool) {
	h := maphash.Hash{}
	h.SetSeed(i.seed)
	b := [8]byte{}
	binary.LittleEndian.PutUint64(b[:], key)
	return i.GetWithHash(key, h.Sum64())
}

func (i *Interner) GetWithHash(key interface{}, hash uint64) (interface{}, bool) {
	blockNum := hash % 127
	block := &i.maps[blockNum]
	return block.get(key)
}

func (b *block) get(key interface{}) (interface{}, bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	if b.elements == nil {
		return 0, false
	}
	idx, present := b.elements[key]
	if !present {
		return 0, false
	}

	elem := &b.storage[idx]
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
	if b.elements == nil {
		return false
	}
	idx, present := b.elements[key]
	if !present {
		return false
	}

	elem := &b.storage[idx]
	if atomic.LoadUint32(&elem.used) != 0 {
		atomic.StoreUint32(&elem.used, 0)
	}

	return true
}

func (i *Interner) Len() uint64 {
	return atomic.LoadUint64(&i.count)
}
