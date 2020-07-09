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

	cache.InsertString("1", 1)
	val, found := cache.GetString("1")

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

	val, found := cache.GetString("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}

	cache.InsertString("key", 1)

	val, found = cache.GetString("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}
}

func TestEviction(t *testing.T) {
	t.Parallel()

	cache := WithMax(10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		cache.InsertString(key, int64(i))
		if i != 5 {
			cache.GetString(key)
		}
	}

	cache.InsertString("100", 100)
	cache.GetString("100")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, found := cache.GetString(key)
		if i != 5 && (!found || val != int64(i)) {
			t.Errorf("missing value %d, got %d", i, val)
		} else if i == 5 && found {
			t.Errorf("5 not evicted")
		}
		if i == 2 {
			cache.Unmark(key)
		}
	}

	val, found := cache.GetString("100")
	if !found || val != 100 {
		t.Errorf("missing value 100, got %d", val)
	}

	cache.InsertString("101", 101)
	cache.GetString("101")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, found := cache.GetString(key)
		if i != 5 && i != 2 && (!found || val != int64(i)) {
			t.Errorf("missing value %d, (found: %v) got %d", i, found, val)
		} else if (i == 5 || i == 2) && found {
			t.Errorf("%d not evicted", i)
		}
	}

	val, found = cache.GetString("100")
	if !found || val != 100 {
		t.Errorf("missing value 100, got %d", val)
	}
	val, found = cache.GetString("101")
	if !found || val != 101 {
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
			cache.InsertString(key, r+1)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < ntest; i++ {
			r := rand.Int63()
			key := fmt.Sprintf("%d", r)
			if val, found := cache.GetString(key); found && val != r+1 {
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
