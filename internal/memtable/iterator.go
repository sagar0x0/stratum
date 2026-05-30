package memtable

import "bytes"

type SkipListIterator struct {
	list    *SkipList
	current *node
}

func (it *SkipListIterator) Valid() bool {
	return it.current != nil
}

func (it *SkipListIterator) Key() []byte {
	return it.current.key
}

func (it *SkipListIterator) Value() []byte {
	return it.current.value
}

func (it *SkipListIterator) Next() {
	if it.current != nil {
		it.list.mu.RLock()
		defer it.list.mu.RUnlock()
		it.current = it.current.forward[0]
	}
}

func (it *SkipListIterator) SeekToFirst() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()
	it.current = it.list.head.forward[0]
}

func (it *SkipListIterator) Seek(target []byte) {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	current := it.list.head
	for i := it.list.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, target) < 0 {
			current = current.forward[i]
		}
	}
	it.current = current.forward[0]
}
