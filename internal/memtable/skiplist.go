package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/sagar0x0/stratum/internal/mvcc"
)

const (
	MaxLevel    = 16
	Probability = 0.25
)

type node struct {
	key     []byte
	value   []byte
	forward []*node
}

type SkipList struct {
	head    *node
	level   int
	size    int64
	memSize int64
	mu      sync.RWMutex
	rng     *rand.Rand
}

func NewSkipList() *SkipList {
	return &SkipList{
		head: &node{forward: make([]*node, MaxLevel)},
		rng:  rand.New(rand.NewSource(1)),
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for sl.rng.Float64() < Probability && level < MaxLevel {
		level++
	}
	return level
}

func (sl *SkipList) Put(key, value []byte) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*node, MaxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && mvcc.CompareKeys(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	if current != nil && bytes.Equal(current.key, key) {
		oldMem := len(current.value)
		current.value = value
		atomic.AddInt64(&sl.memSize, int64(len(value)-oldMem))
		return
	}

	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}

	newNode := &node{
		key:     key,
		value:   value,
		forward: make([]*node, level),
	}

	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	atomic.AddInt64(&sl.size, 1)
	atomic.AddInt64(&sl.memSize, int64(32+len(key)+len(value)+8*level))
}

func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && mvcc.CompareKeys(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]
	if current != nil && bytes.Equal(current.key, key) {
		return current.value, true
	}
	return nil, false
}

func (sl *SkipList) Delete(key []byte) {
	sl.Put(key, nil)
}

func (sl *SkipList) Contains(key []byte) bool {
	_, found := sl.Get(key)
	return found
}

func (sl *SkipList) Len() int64 {
	return atomic.LoadInt64(&sl.size)
}

func (sl *SkipList) MemoryUsage() int64 {
	return atomic.LoadInt64(&sl.memSize)
}

func (sl *SkipList) NewIterator() *SkipListIterator {
	it := &SkipListIterator{
		list:    sl,
		current: nil,
	}
	return it
}
