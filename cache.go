package smolcache

import (
	"sync"
	"sync/atomic"

	"hash/maphash"
)

const numBlocks = 127

// CLOCK based approximate LRU storing mappings from strings to
// int64s designed for concurrent usage.
// It is sharded into a number of blocks, each of which can be
// locked independently. Gets only require a read lock on the
// individual block the element can be found in. When the cache is
// not full, Insert only requires a write lock on the individual
// block. Eviction locks blocks one at a time looking for a value
// that's valid to evict/
type Interner struct {
	maps [numBlocks]block

	max  uint64
	seed maphash.Seed

	// padding so that the count, which changes frequently, doesn't
	// share a cache line with the max and seed, which are read only
	_padding [6]uint64

	// number of elements stored in this cache. Is set using atomic atomic ops
	count uint64

	clockLock sync.Mutex

	// CLOCK sweep state, guarded by clockLock
	clock uint8
}

// An individual block of the cache.
type block struct {
	lock sync.RWMutex
	// guarded by lock, indexes into elements
	mapping map[string]int

	// memory for all of the Elements stored within the cache.
	// not every element is used, any marked with element.used = 2
	// is free for reuse
	elements []Element

	// the first element free to be resused by Insert,
	// index into elements, or -1 if there are no free elements
	first_free_element int

	// CLOCK sweep state, guarded by clockLock

	// the next element the evictor should check
	// index into elements
	next int
	// pad blocks out to be cache aligned
	_padding [7]uint64
}

// a single Element stored by the cache
type Element struct {
	// the key for this element
	key string

	// the value(s) stored by this element
	// (since we need to pad this `struct` out to cache alignment anyway,
	// there's little downside to storing both a string and int64 here, and
	// letting the user ignore whichever one they don't want to use)
	int_value int64
	str_value string

	// index of this element in elements
	idx int

	// when this is on the freelist, the index of the next free element
	// in block.elements
	next_free int

	//CLOCK marker if this is recently used
	used uint32
}

const (
	elementUnhit = 0
	elementHit   = 1
	elementFree  = 2
)

const invalidIndex = -1

// create an Interner that can hold at most `max` elements
func WithMax(max uint64) *Interner {
	cache := &Interner{
		max:  max,
		seed: maphash.MakeSeed(),
		maps: [numBlocks]block{},
	}
	for i := 0; i < numBlocks; i++ {
		cache.maps[i].first_free_element = -1
	}
	return cache
}

// insert a key into the cache, evicting if there would be more than
// max elements stored. If the key was already in the map this is a nop.
func (i *Interner) Insert(key string, int_value int64, str_value string) {
	newSize := atomic.AddUint64(&i.count, 1)
	needsEvict := newSize > i.max
	if needsEvict {
		i.evict()
	}

	h := maphash.Hash{}
	h.SetSeed(i.seed)
	h.WriteString(key)
	blockNum := h.Sum64() % numBlocks
	block := &i.maps[blockNum]
	block.insert(key, int_value, str_value)
}

func (b *block) insert(key string, int_value int64, str_value string) bool {
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
	if b.first_free_element != invalidIndex {
		// we have free elements in b.elements, take the first one and use that
		// remove the element from the free list
		element := &b.elements[b.first_free_element]
		b.first_free_element = element.next_free
		element.next_free = invalidIndex

		element.key = key
		element.int_value = int_value
		element.str_value = str_value
		element.used = elementUnhit
		idx = element.idx
	} else {
		// no free elements, add a new one
		idx = len(b.elements)
		b.elements = append(b.elements, Element{
			key:       key,
			int_value: int_value,
			str_value: str_value,
			idx:       idx,
			used:      elementUnhit,
		})
	}
	b.mapping[key] = idx
	return true
}

// evict the least recently used element
func (i *Interner) evict() {
	i.clockLock.Lock()
	defer i.clockLock.Unlock()

	// if this cache is empty, just return
	if i.count == 0 {
		return
	}

	// try to evict from each block in roundrobin fashion
	// NOTE: this can take forever if other threads are continually accessing
	//       all of the other elements. However, in that case progress is still
	//       being made, and arguably the real issue is that the cache isn't
	//       large enough
	// TODO: it may may to add an option to expand the cache on that case
	for {
		block := &i.maps[i.clock%numBlocks]
		evicted, reachedEnd := block.tryEvict()
		if reachedEnd {
			i.clock += 1
		}
		if evicted {
			// idiomatic way to perform atomic uint64 subtractions,
			// see `atomic.AddUint64` docs
			atomic.AddUint64(&i.count, ^uint64(0))
			break
		}
	}
}

// search for an element to evict within a block
func (b *block) tryEvict() (evicted bool, reachedEnd bool) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.next == len(b.elements) {
		b.next = 0
		if b.next == len(b.elements) {
			// this block is empty, move on to the next
			return false, true
		}
	}

	evicted = false
	reachedEnd = false
	// loop until we find an element to evict or we run out of elements in this
	// block
	for !evicted && !reachedEnd {
		idx := b.next
		b.next = idx + 1

		reachedEnd = b.next == len(b.elements)

		elem := &b.elements[idx]
		if elem.used == elementHit {
			// if the element is hit, mark it as unhit
			elem.used = elementUnhit
		} else if elem.used == elementUnhit {
			// if the element is unhit, remove it, we're done
			elem.used = elementFree
			delete(b.mapping, elem.key)
			elem.int_value = 0
			elem.str_value = ""
			elem.next_free = b.first_free_element
			b.first_free_element = idx

			evicted = true
		}
	}

	return evicted, reachedEnd
}

// get the value(s) associated with a key
func (i *Interner) Get(key string) (int64, string, bool) {
	h := maphash.Hash{}
	h.SetSeed(i.seed)
	h.WriteString(key)
	blockNum := h.Sum64() % numBlocks
	block := &i.maps[blockNum]
	return block.get(key)
}

func (b *block) get(key string) (int64, string, bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	if b.mapping == nil {
		return 0, "", false
	}
	idx, present := b.mapping[key]
	if !present {
		return 0, "", false
	}
	elem := &b.elements[idx]

	if atomic.LoadUint32(&elem.used) == elementUnhit {
		atomic.StoreUint32(&elem.used, elementHit)
	}

	return elem.int_value, elem.str_value, true
}

// set an element as being able to be evicted
func (i *Interner) Unmark(key string) bool {
	h := maphash.Hash{}
	h.SetSeed(i.seed)
	h.WriteString(key)
	blockNum := h.Sum64() % numBlocks
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

	if atomic.LoadUint32(&elem.used) != elementUnhit {
		atomic.StoreUint32(&elem.used, elementUnhit)
	}

	return true
}

func (i *Interner) Len() uint64 {
	return atomic.LoadUint64(&i.count)
}
