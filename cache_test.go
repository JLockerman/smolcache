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
	cache.Insert("1", 1, "foo")
	ival, sval, found := cache.Get("1")

	// then
	if !found {
		t.Error("no value found")
	}
	if ival != 1 {
		t.Errorf("expected %d found %d", 1, ival)
	}
	if sval != "foo" {
		t.Errorf("expected %s found %s", "foo", sval)
	}
}

func TestEntryNotFound(t *testing.T) {
	t.Parallel()

	cache := WithMax(100)

	val, sval, found := cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}
	if val != 0 || sval != "" {
		t.Errorf("unexpected val %v, %v for noexistent key", val, sval)
	}

	cache.Insert("key", 1, "foo")

	val, sval, found = cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}
}

func TestEviction(t *testing.T) {
	t.Parallel()

	cache := WithMax(10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		cache.Insert(key, int64(i), fmt.Sprintf("str %d", i))
		if i != 5 {
			// ensure all value except for 5 are marked as used
			val, sval, found := cache.Get(key)
			if !found || val != int64(i) || sval != fmt.Sprintf("str %d", i) {
				t.Errorf("missing value %d, got (%d, %s)", i, val, sval)
			}
		}
	}

	// this should evict 5
	cache.Insert("100", 100, fmt.Sprintf("str %d", 100))
	// ensure 100 is marked as used
	cache.Get("100")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, sval, found := cache.Get(key)
		// ensure all values are marked as used
		if i != 5 && (!found || val != int64(i) || sval != fmt.Sprintf("str %d", i)) {
			t.Errorf("missing value %d, got (%d, %s)\n%+v", i, val, sval, cache)
		} else if i == 5 && found {
			t.Errorf("5 not evicted")
		}
		// unmark 2
		if i == 2 {
			cache.Unmark(key)
		}
	}

	val, sval, found := cache.Get("100")
	if !found || val != 100 || sval != fmt.Sprintf("str %d", 100) {
		t.Errorf("missing value 100, got (%d, %s)", val, sval)
	}

	t.Logf("cache %+v", cache)

	cache.Insert("101", 101, fmt.Sprintf("str %d", 101))
	cache.Get("101")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, sval, found := cache.Get(key)
		if i != 5 && i != 2 && (!found || val != int64(i) || sval != fmt.Sprintf("str %d", i)) {
			t.Errorf("missing value %d, (found: %v) got (%d, %s)\n%+v", i, found, val, sval, cache)
		} else if (i == 5 || i == 2) && found {
			t.Errorf("%d not evicted", i)
		}
	}

	val, sval, found = cache.Get("100")
	if !found || val != 100 || sval != fmt.Sprintf("str %d", 100) {
		t.Errorf("missing value 100, got %d", val)
	}
	val, sval, found = cache.Get("101")
	if !found || val != 101 || sval != fmt.Sprintf("str %d", 101) {
		t.Errorf("missing value 101, got %d", val)
	}
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
			cache.Insert(key, r+1, "")
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < ntest; i++ {
			r := rand.Int63()
			key := fmt.Sprintf("%d", r)
			if val, _, found := cache.Get(key); found && val != r+1 {
				t.Errorf("got %s ->\n %x\n expected:\n %x\n ", key, val, r+1)
			}
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestBlockCacheAligned(t *testing.T) {
	blockSize := unsafe.Sizeof(block{})
	if blockSize%64 != 0 {
		t.Errorf("unaligned block size: %d", blockSize)
	}
}

func TestElementCacheAligned(t *testing.T) {
	elementSize := unsafe.Sizeof(Element{})
	if elementSize%64 != 0 {
		t.Errorf("unaligned element size: %d", elementSize)
	}
}

func TestCountOffset(t *testing.T) {
	seedOffset := unsafe.Offsetof(Interner{}.seed)
	countOffset := unsafe.Offsetof(Interner{}.count)
	if seedOffset/64 == countOffset/64 {
		t.Errorf("seed and count on same cache line\nseed @ %d (%d)\noffset @ %d (%d)", seedOffset, seedOffset/64, countOffset, countOffset/64)
	}
}
