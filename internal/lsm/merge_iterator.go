package lsm

import (
	"bytes"
	"container/heap"
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
	it    Iterator
	idx   int // To ensure stable sort for iterators with same key
}

// iterHeap implements heap.Interface for a min-heap of iterators.
type iterHeap []iterHeapItem

func (h iterHeap) Len() int { return len(h) }
func (h iterHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].it.Key(), h[j].it.Key())
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
	m.advance()
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
	m.advance()
}

// Next moves to the next unique key.
func (m *MergeIterator) Next() {
	if !m.valid {
		return
	}
	// The current key has been consumed.
	// Since there could be duplicate keys from other iterators in the heap,
	// we pop and advance them if they match the current key.
	// Actually, advance() already handles dropping duplicates.
	// But to get to the *next* key, we just pop the current one.
	
	// Wait, the item currently on top of the heap is the one we returned.
	if m.hp.Len() > 0 {
		item := heap.Pop(m.hp).(iterHeapItem)
		item.it.Next()
		if item.it.Valid() {
			heap.Push(m.hp, item)
		}
	}
	m.advance()
}

func (m *MergeIterator) advance() {
	if m.hp.Len() == 0 {
		m.valid = false
		return
	}

	top := (*m.hp)[0]
	m.currentKey = top.it.Key()
	m.currentVal = top.it.Value()
	m.valid = true

	// Drop shadowed versions of the same key
	for m.hp.Len() > 0 && bytes.Equal((*m.hp)[0].it.Key(), m.currentKey) {
		if (*m.hp)[0].idx == top.idx {
			// This is the top item itself, don't advance it yet.
			// Next() will advance it.
			// Wait, the top item is at index 0. We shouldn't advance it in advance()
			// if it's the exact same iterator.
			break
		}

		// It's an older version from a different iterator. Advance it.
		item := heap.Pop(m.hp).(iterHeapItem)
		item.it.Next()
		if item.it.Valid() {
			heap.Push(m.hp, item)
		}
	}
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
