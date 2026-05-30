package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPutAndGet(t *testing.T) {
	sl := NewSkipList()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		val := []byte(fmt.Sprintf("val%04d", i))
		sl.Put(key, val)
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		val, found := sl.Get(key)
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprintf("val%04d", i)), val)
	}
}

func TestPutOverwrite(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("key"), []byte("val1"))
	sl.Put([]byte("key"), []byte("val2"))

	val, found := sl.Get([]byte("key"))
	assert.True(t, found)
	assert.Equal(t, []byte("val2"), val)
}

func TestDelete(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("key"), []byte("val"))
	sl.Delete([]byte("key"))

	val, found := sl.Get([]byte("key"))
	assert.True(t, found)
	assert.Nil(t, val)
}

func TestDeleteNonExistent(t *testing.T) {
	sl := NewSkipList()
	sl.Delete([]byte("key"))

	val, found := sl.Get([]byte("key"))
	assert.True(t, found)
	assert.Nil(t, val)
}

func TestOrdering(t *testing.T) {
	sl := NewSkipList()
	keys := []string{"d", "a", "c", "e", "b"}
	for _, k := range keys {
		sl.Put([]byte(k), []byte(k))
	}

	it := sl.NewIterator()
	it.SeekToFirst()

	expected := []string{"a", "b", "c", "d", "e"}
	for _, k := range expected {
		assert.True(t, it.Valid())
		assert.Equal(t, []byte(k), it.Key())
		it.Next()
	}
	assert.False(t, it.Valid())
}

func TestIteratorSeek(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("a"), []byte("1"))
	sl.Put([]byte("c"), []byte("2"))
	sl.Put([]byte("e"), []byte("3"))

	it := sl.NewIterator()

	it.Seek([]byte("b"))
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("c"), it.Key())

	it.Seek([]byte("c"))
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("c"), it.Key())

	it.Seek([]byte("d"))
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("e"), it.Key())
}

func TestIteratorSeekPastEnd(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("a"), []byte("1"))

	it := sl.NewIterator()
	it.Seek([]byte("b"))
	assert.False(t, it.Valid())
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	sl := NewSkipList()
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(writerID)))
			for {
				select {
				case <-stop:
					return
				default:
					k := []byte(fmt.Sprintf("k%d", rng.Intn(1000)))
					sl.Put(k, k)
				}
			}
		}(i)
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				select {
				case <-stop:
					return
				default:
					k := []byte(fmt.Sprintf("k%d", rng.Intn(1000)))
					sl.Get(k)
				}
			}
		}()
	}

	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()
}

func TestConcurrentWriteContention(t *testing.T) {
	sl := NewSkipList()
	var wg sync.WaitGroup

	numWriters := 8
	itemsPerWriter := 1000

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < itemsPerWriter; j++ {
				k := []byte(fmt.Sprintf("k%04d-%04d", id, j))
				sl.Put(k, []byte("v"))
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int64(numWriters*itemsPerWriter), sl.Len())
}

func TestMemoryUsageTracking(t *testing.T) {
	sl := NewSkipList()
	assert.Equal(t, int64(0), sl.MemoryUsage())

	sl.Put([]byte("k"), []byte("v"))
	mem1 := sl.MemoryUsage()
	assert.Greater(t, mem1, int64(0))

	sl.Put([]byte("k2"), bytes.Repeat([]byte("v"), 1000))
	mem2 := sl.MemoryUsage()
	assert.Greater(t, mem2, mem1+1000)
}

func TestLargeDataset(t *testing.T) {
	sl := NewSkipList()
	for i := 0; i < 100000; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		sl.Put(k, k)
	}

	assert.Equal(t, int64(100000), sl.Len())

	it := sl.NewIterator()
	it.SeekToFirst()
	count := 0
	for it.Valid() {
		count++
		it.Next()
	}
	assert.Equal(t, 100000, count)
}
