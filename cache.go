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
	elements map[interface{}]*Element
	max      uint64
	// only safe to not use a pointer since blocks never move
	sweep List

	// CLOCK sweep state, must have write lock
	prev *Element

	lock  sync.RWMutex
	count uint64
}

func WithMax(max uint64) *Interner {
	return &Interner{
		max: max,
	}
}

func WithMaxAndShards(max uint64, shards int) *Interner {
	if max < 1 {
		panic("must have max greater than 0")
	}
	//TODO variable number of shards
	return &Interner{
		max: max,
	}
}

func (i *Interner) Insert(key interface{}, value interface{}) (interface{}, bool) {
	newSize := atomic.AddUint64(&i.count, 1)
	needsEvict := newSize > i.max
	var elem *Element
	if !needsEvict {
		elem = makeElement(key, value)
	}
	i.lock.Lock()
	defer i.lock.Unlock()
	if needsEvict {
		elem = i.evict()
		elem.set(key, value)
	}

	if i.elements == nil {
		i.elements = make(map[interface{}]*Element)
	}
	val, present := i.elements[key]
	if present {
		return val.Value, false
	}
	i.sweep.PushBack(elem)
	i.elements[key] = elem
	return value, true
}

func (i *Interner) evict() *Element {
	if i.count == 0 {
		return nil
	}
	for {
		elem, evicted := i.tryEvict()
		if evicted {
			atomic.AddUint64(&i.count, ^uint64(0))
			return elem
		}
	}
}

func (i *Interner) tryEvict() (elem *Element, evicted bool) {
	if i.prev == nil || i.prev.Next() == nil {
		i.prev = i.sweep.Root()
	}

	elem = i.prev.Next()
	if elem == nil {
		return nil, false
	}

	evicted = false
	reachedEnd := false
	for !evicted && !reachedEnd {
		if elem.used != 0 {
			elem.used = 0
			i.prev = elem
			elem = i.prev.Next()
			reachedEnd = elem == nil
		} else {
			key, _ := i.sweep.RemoveNext(i.prev)
			delete(i.elements, key)
			evicted = true
		}
	}

	return
}

func (i *Interner) Get(key interface{}) (interface{}, bool) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	if i.elements == nil {
		return 0, false
	}
	elem, present := i.elements[key]
	if !present {
		return 0, false
	}

	if atomic.LoadUint32(&elem.used) == 0 {
		atomic.StoreUint32(&elem.used, 1)
	}

	return elem.Value, true
}

func (i *Interner) Unmark(key string) bool {
	i.lock.RLock()
	defer i.lock.RUnlock()
	if i.elements == nil {
		return false
	}
	elem, present := i.elements[key]
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
