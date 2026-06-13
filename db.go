package stratum

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sagar0x0/stratum/internal/lsm"
	"github.com/sagar0x0/stratum/internal/memtable"
	"github.com/sagar0x0/stratum/internal/wal"
)

var (
	ErrNotFound = errors.New("stratum: key not found")
	ErrClosed   = errors.New("stratum: db closed")
)

type DB struct {
	opts    Options
	wal     *wal.Writer
	gc      *wal.GroupCommitter
	manager *memtable.Manager
	lsm     *lsm.LSMTree

	mu     sync.RWMutex
	closed bool
}

func Open(opts Options) (*DB, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(opts.Dir, "wal.log")

	res, err := wal.Recover(walPath)
	if err != nil {
		return nil, err
	}

	w, err := wal.NewWriter(walPath)
	if err != nil {
		return nil, err
	}

	gcOpts := []wal.GroupCommitOption{
		wal.WithMaxBatchSize(opts.WALMaxBatchSize),
		wal.WithMaxBatchDelay(opts.WALMaxBatchDelay),
	}
	gc := wal.NewGroupCommitter(w, gcOpts...)
	gc.Start()

	lsmOpts := lsm.LSMOptions{
		Dir:              opts.Dir,
		BlockSize:        opts.SSTableBlockSize,
		BloomBitsPerKey:  opts.BloomBitsPerKey,
		BlockCacheSize:   opts.BlockCacheSize,
		CompactionRateMB: opts.CompactionRateMB,
		L0StallTrigger:   opts.L0StallTrigger,
	}
	
	lsmTree, err := lsm.NewLSMTree(lsmOpts)
	if err != nil {
		return nil, err
	}
	lsmTree.StartCompaction()

	db := &DB{
		opts: opts,
		wal:  w,
		gc:   gc,
		lsm:  lsmTree,
	}

	flushFn := func(mt *memtable.MemTable) error {
		if err := db.lsm.Flush(mt); err != nil {
			return err
		}

		if err := db.rotateWAL(); err != nil {
			log.Printf("failed to rotate WAL: %v", err)
		}
		return nil
	}

	manager := memtable.NewManager(gc, opts.MemTableSize, flushFn)

	for _, batch := range res.Batches {
		if err := manager.ApplyRecoveredBatch(batch); err != nil {
			return nil, err
		}
	}

	manager.Start()
	db.manager = manager

	return db, nil
}

func (db *DB) rotateWAL() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.gc.Stop()
	db.wal.Close()

	walPath := filepath.Join(db.opts.Dir, "wal.log")
	rotatedPath := filepath.Join(db.opts.Dir, fmt.Sprintf("wal_%d.log", time.Now().UnixNano()))
	os.Rename(walPath, rotatedPath)

	w, err := wal.NewWriter(walPath)
	if err != nil {
		return err
	}
	db.wal = w

	gcOpts := []wal.GroupCommitOption{
		wal.WithMaxBatchSize(db.opts.WALMaxBatchSize),
		wal.WithMaxBatchDelay(db.opts.WALMaxBatchDelay),
	}
	db.gc = wal.NewGroupCommitter(w, gcOpts...)
	db.gc.Start()

	os.Remove(rotatedPath)

	return nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}

	return db.manager.Put(key, value)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	db.mu.RUnlock()

	val, found := db.manager.Get(key)
	if found {
		if val == nil {
			return nil, ErrNotFound
		}
		return val, nil
	}

	val, found, err := db.lsm.Get(key)
	if err != nil {
		return nil, err
	}
	if found {
		if val == nil {
			return nil, ErrNotFound
		}
		return val, nil
	}

	return nil, ErrNotFound
}

func (db *DB) Delete(key []byte) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}

	return db.manager.Delete(key)
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.closed = true

	db.manager.Stop()
	db.gc.Stop()
	db.wal.Close()
	return db.lsm.Close()
}
