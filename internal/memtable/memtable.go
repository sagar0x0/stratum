package memtable

import (
	"errors"
	"time"

	"github.com/sagar0x0/stratum/internal/wal"
)

var ErrMemTableFull = errors.New("memtable: is full or frozen")

type MemTable struct {
	sl        *SkipList
	maxSize   int64
	frozen    bool
	createdAt time.Time
}

func NewMemTable(maxSize int64) *MemTable {
	return &MemTable{
		sl:        NewSkipList(),
		maxSize:   maxSize,
		createdAt: time.Now(),
	}
}

func (mt *MemTable) Put(key, value []byte) error {
	if mt.frozen || mt.sl.MemoryUsage() >= mt.maxSize {
		return ErrMemTableFull
	}
	mt.sl.Put(key, value)
	return nil
}

func (mt *MemTable) Get(key []byte) ([]byte, bool) {
	return mt.sl.Get(key)
}

func (mt *MemTable) Delete(key []byte) error {
	if mt.frozen || mt.sl.MemoryUsage() >= mt.maxSize {
		return ErrMemTableFull
	}
	mt.sl.Delete(key)
	return nil
}

func (mt *MemTable) ShouldFlush() bool {
	return mt.sl.MemoryUsage() >= mt.maxSize
}

func (mt *MemTable) Freeze() {
	mt.frozen = true
}

func (mt *MemTable) IsFrozen() bool {
	return mt.frozen
}

func (mt *MemTable) NewIterator() *SkipListIterator {
	return mt.sl.NewIterator()
}

func (mt *MemTable) ApplyBatch(batch *wal.Batch) error {
	for _, entry := range batch.Entries {
		if entry.Op == wal.OpPut {
			if err := mt.Put(entry.Key, entry.Value); err != nil {
				return err
			}
		} else if entry.Op == wal.OpDelete {
			if err := mt.Delete(entry.Key); err != nil {
				return err
			}
		}
	}
	return nil
}
