package stratum

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

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

	flushFn := func(mt *memtable.MemTable) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	manager := memtable.NewManager(gc, opts.MemTableSize, flushFn)

	for _, batch := range res.Batches {
		if err := manager.ApplyRecoveredBatch(batch); err != nil {
			return nil, err
		}
	}

	manager.Start()

	return &DB{
		opts:    opts,
		wal:     w,
		gc:      gc,
		manager: manager,
	}, nil
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
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}

	val, found := db.manager.Get(key)
	if !found || val == nil {
		return nil, ErrNotFound
	}
	return val, nil
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
	return db.wal.Close()
}
