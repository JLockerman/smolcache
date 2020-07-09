package smolcache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"unsafe"
)

func TestWriteAndGetOnCache(t *testing.T) {
	t.Parallel()

	cache := WithMax(100)

	cache.Insert("1", 1)
	val, found := cache.Get("1")

	// then
	if !found {
		t.Error("no value found")
	}
	if val != 1 {
		t.Errorf("expected %d found %d", 1, val)
	}
}

func TestEntryNotFound(t *testing.T) {
	t.Parallel()

	cache := WithMax(100)

	val, found := cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}

	cache.Insert("key", 1)

	val, found = cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}
}

func TestEviction(t *testing.T) {
	t.Parallel()

	cache := WithMax(10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		cache.Insert(key, int64(i))
		if i != 5 {
			cache.Get(key)
		}
	}

	cache.Insert("100", 100)
	cache.Get("100")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, found := cache.Get(key)
		if i != 5 && (!found || val != int64(i)) {
			t.Errorf("missing value %d, got %d", i, val)
		} else if i == 5 && found {
			t.Errorf("5 not evicted")
		}
		if i == 2 {
			cache.Unmark(key)
		}
	}

	val, found := cache.Get("100")
	if !found || val != 100 {
		t.Errorf("missing value 100, got %d", val)
	}

	cache.Insert("101", 101)
	cache.Get("101")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, found := cache.Get(key)
		if i != 5 && i != 2 && (!found || val != int64(i)) {
			t.Errorf("missing value %d, (found: %v) got %d", i, found, val)
		} else if (i == 5 || i == 2) && found {
			t.Errorf("%d not evicted", i)
		}
	}

	val, found = cache.Get("100")
	if !found || val != 100 {
		t.Errorf("missing value 100, got %d", val)
	}
	val, found = cache.Get("101")
	if !found || val != 101 {
		t.Errorf("missing value 101, got %d", val)
	}
}

func printCache(cache *Interner, t *testing.T) {
	str := "["
	for k, v := range cache.elements {
		str = fmt.Sprintf("%s\n\t%v: %v, ", str, k, v)
	}
	t.Logf("%s]", str)
}

func TestCacheGetRandomly(t *testing.T) {
	t.Parallel()

	cache := WithMax(10000)
	var wg sync.WaitGroup
	var ntest = 800000
	wg.Add(2)
	go func() {
		for i := 0; i < ntest; i++ {
			r := rand.Int63() % 20000
			key := fmt.Sprintf("%d", r)
			cache.Insert(key, r+1)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < ntest; i++ {
			r := rand.Int63()
			key := fmt.Sprintf("%d", r)
			if val, found := cache.Get(key); found && val != r+1 {
				t.Errorf("got %s ->\n %x\n expected:\n %x\n ", key, val, r+1)
			}
		}
		wg.Done()
	}()
	wg.Wait()
}

// func TestBlockCacheAligned(t *testing.T) {
// 	blockSize := unsafe.Sizeof(block{})
// 	if blockSize%64 != 0 {
// 		t.Errorf("unaligned block size: %d", blockSize)
// 	}
// }

func TestElementCacheAligned(t *testing.T) {
	elementSize := unsafe.Sizeof(Element{})
	if elementSize%64 != 0 {
		t.Errorf("unaligned element size: %d", elementSize)
	}
}

func TestCountOffset(t *testing.T) {
	elementsOffset := unsafe.Offsetof(Interner{}.elements)
	lockOffset := unsafe.Offsetof(Interner{}.lock)
	t.Logf("elem offset %d", elementsOffset)
	t.Logf("lock offset %d", lockOffset)
	if elementsOffset/64 == lockOffset/64 {
		t.Errorf("read-mostly and mutable on the same line\nseed @ %d (%d)\noffset @ %d (%d)", elementsOffset, elementsOffset/64, lockOffset, elementsOffset/64)
	}
}
