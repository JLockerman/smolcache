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
	// guarded by lock
	elements map[string]*Element

	// only safe to not use a pointer since blocks never move
	sweep List

	// CLOCK sweep state, guarded by clockLock
	next *Element
	// pad blocks out to be cache aligned
	_padding [16]byte
}

func WithMax(max uint64) *Interner {
	return &Interner{
		max:  max,
		seed: maphash.MakeSeed(),
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
	if b.elements == nil {
		b.elements = make(map[string]*Element)
	}
	_, present := b.elements[key]
	if present {
		return false
	}
	elem := b.sweep.PushBack(key, value)
	b.elements[key] = elem
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
	if b.next == nil {
		b.next = b.sweep.Front()
		if b.next == nil {
			return false, true
		}
	}

	evicted = false
	reachedEnd = false
	for !evicted && !reachedEnd {
		elem := b.next
		b.next = elem.Next()
		reachedEnd = b.next == nil
		if elem.used != 0 {
			elem.used = 0
		} else {
			key, _ := b.sweep.Remove(elem)
			delete(b.elements, key)
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
	if b.elements == nil {
		return 0, false
	}
	elem, present := b.elements[key]
	if !present {
		return 0, false
	}

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
	elem, present := b.elements[key]
	if !present {
		return false
	}

	if atomic.LoadUint32(&elem.used) != 0 {
		atomic.StoreUint32(&elem.used, 0)
	}

	return true
}

func (i *Interner) Len() uint64 {
	return atomic.LoadUint64(&i.count)
}
