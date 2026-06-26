package lsm

import (
	"bytes"
	"container/heap"

	"github.com/sagar0x0/stratum/internal/mvcc"
)

// Iterator represents a generic iterator for KVs.
type Iterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next()
	SeekToFirst()
	Seek(target []byte)
}

// iterHeapItem is an item in the min-heap.
type iterHeapItem struct {
	it  Iterator
	idx int // To ensure stable sort for iterators with same key
}

// iterHeap implements heap.Interface for a min-heap of iterators.
type iterHeap []iterHeapItem

func (h iterHeap) Len() int { return len(h) }
func (h iterHeap) Less(i, j int) bool {
	cmp := mvcc.CompareKeys(h[i].it.Key(), h[j].it.Key())
	if cmp == 0 {
		// Newest data comes from lower-indexed iterators (e.g. MemTable first, then L0, then L1)
		return h[i].idx < h[j].idx
	}
	return cmp < 0
}
func (h iterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *iterHeap) Push(x interface{}) {
	*h = append(*h, x.(iterHeapItem))
}
func (h *iterHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// MergeIterator merges multiple Iterators into a single sorted Iterator.
// It deduplicates keys: for duplicate keys the value from the lowest-index
// (newest) iterator wins.
type MergeIterator struct {
	iters []Iterator
	hp    *iterHeap

	currentKey []byte
	currentVal []byte
	valid      bool
}

// NewMergeIterator creates a new MergeIterator.
// The iterators should be passed in order of newest to oldest data.
func NewMergeIterator(iters ...Iterator) *MergeIterator {
	m := &MergeIterator{
		iters: iters,
		hp:    &iterHeap{},
	}
	return m
}

// SeekToFirst moves to the very first key.
func (m *MergeIterator) SeekToFirst() {
	*m.hp = iterHeap{}
	for i, it := range m.iters {
		it.SeekToFirst()
		if it.Valid() {
			heap.Push(m.hp, iterHeapItem{it: it, idx: i})
		}
	}
	m.findSmallest()
}

// Seek moves to the first key >= target.
func (m *MergeIterator) Seek(target []byte) {
	*m.hp = iterHeap{}
	for i, it := range m.iters {
		it.Seek(target)
		if it.Valid() {
			heap.Push(m.hp, iterHeapItem{it: it, idx: i})
		}
	}
	m.findSmallest()
}

// findSmallest sets currentKey/currentVal from the heap top, without
// advancing any iterator. This is used after initial positioning.
func (m *MergeIterator) findSmallest() {
	if m.hp.Len() == 0 {
		m.valid = false
		return
	}

	top := (*m.hp)[0]
	m.currentKey = append(m.currentKey[:0], top.it.Key()...)
	m.currentVal = top.it.Value()
	m.valid = true
}

// Next moves to the next unique key.
func (m *MergeIterator) Next() {
	if !m.valid {
		return
	}

	// Pop and advance ALL iterators whose current key equals m.currentKey.
	// This ensures deduplication: if 3 iterators all have "keyA", we skip
	// past "keyA" in all of them before reading the next smallest key.
	for m.hp.Len() > 0 && bytes.Equal((*m.hp)[0].it.Key(), m.currentKey) {
		item := heap.Pop(m.hp).(iterHeapItem)
		item.it.Next()
		if item.it.Valid() {
			heap.Push(m.hp, item)
		}
	}

	m.findSmallest()
}

// Valid returns true if the iterator is currently valid.
func (m *MergeIterator) Valid() bool {
	return m.valid
}

// Key returns the current key.
func (m *MergeIterator) Key() []byte {
	return m.currentKey
}

// Value returns the current value.
func (m *MergeIterator) Value() []byte {
	return m.currentVal
}
