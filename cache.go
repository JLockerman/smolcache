package smolcache

import (
	"sync"
	"sync/atomic"
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
	lock sync.RWMutex
	// stores indexes into storage
	elements map[interface{}]int
	storage  []Element

	// CLOCK sweep state, must have write lock
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
	if max < 1 {
		panic("must have max greater than 0")
	}
	return &Interner{
		elements: make(map[interface{}]int, max),
		storage:  make([]Element, 0, max),
	}
}

func WithMaxAndShards(max uint64, shards int) *Interner {
	return WithMax(max)
}

func (i *Interner) Insert(key interface{}, value interface{}) (interface{}, bool) {
	i.lock.Lock()
	defer i.lock.Unlock()

	idx, present := i.elements[key]
	if present {
		return i.storage[idx].Value, false
	}

	var insertLocation *Element
	var insertIdx int
	if len(i.storage) >= cap(i.storage) {
		insertLocation, insertIdx = i.evict()
		*insertLocation = Element{key: key, Value: value}
	} else {
		insertIdx = len(i.storage)
		i.storage = append(i.storage, Element{key: key, Value: value})
		insertLocation = &i.storage[len(i.storage)-1]
	}

	i.elements[key] = insertIdx
	return value, true
}

func (i *Interner) evict() (insertPtr *Element, insertIdx int) {
	for {
		insertLocation, insertIdx, evicted := i.tryEvict()
		if evicted {
			return insertLocation, insertIdx
		}
	}
}

func (i *Interner) tryEvict() (insertPtr *Element, insertIdx int, evicted bool) {
	if i.next >= len(i.storage) {
		i.next = 0
	}

	evicted = false
	reachedEnd := false
	for !evicted && !reachedEnd {
		elem := &i.storage[i.next]
		if elem.used != 0 {
			elem.used = 0
		} else {
			insertPtr = elem
			insertIdx = i.next
			key := elem.key
			delete(i.elements, key)
			evicted = true
		}
		i.next += 1
		reachedEnd = i.next >= len(i.storage)
	}

	return
}

func (i *Interner) Get(key interface{}) (interface{}, bool) {
	i.lock.RLock()
	defer i.lock.RUnlock()

	idx, present := i.elements[key]
	if !present {
		return 0, false
	}

	elem := &i.storage[idx]
	if atomic.LoadUint32(&elem.used) == 0 {
		atomic.StoreUint32(&elem.used, 1)
	}

	return elem.Value, true
}

func (i *Interner) Unmark(key string) bool {
	i.lock.RLock()
	defer i.lock.RUnlock()

	idx, present := i.elements[key]
	if !present {
		return false
	}

	elem := &i.storage[idx]
	if atomic.LoadUint32(&elem.used) != 0 {
		atomic.StoreUint32(&elem.used, 0)
	}

	return true
}

func (i *Interner) Len() int {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return len(i.storage)
}
