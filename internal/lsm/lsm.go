package lsm

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sagar0x0/stratum/internal/memtable"
	"github.com/sagar0x0/stratum/internal/mvcc"
	"github.com/sagar0x0/stratum/internal/sstable"
)

type LSMOptions struct {
	Dir               string
	BlockSize         int
	BloomBitsPerKey   int
	BlockCacheSize    int64
	CompactionRateMB  int
	MinActiveSnapshot func() uint64
	L0StallTrigger    int
}

// LSMTree orchestrates the components of the LSM-tree.
type LSMTree struct {
	dir      string
	manifest *Manifest
	readers  map[uint64]*sstable.Reader
	cache    *sstable.BlockCache
	opts     LSMOptions

	compactCh chan struct{}
	stopCh    chan struct{}
	compactWg sync.WaitGroup

	mu        sync.RWMutex
	stalled   bool
	stallCond *sync.Cond

	compactor *CompactionExecutor
	stats     *CompactionStats
}

// NewLSMTree creates and initializes a new LSMTree.
func NewLSMTree(opts LSMOptions) (*LSMTree, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}

	manifestPath := filepath.Join(opts.Dir, "MANIFEST")
	manifest, err := OpenManifest(manifestPath)
	if err != nil {
		return nil, err
	}

	cache := sstable.NewBlockCache(opts.BlockCacheSize)

	l := &LSMTree{
		dir:       opts.Dir,
		manifest:  manifest,
		readers:   make(map[uint64]*sstable.Reader),
		cache:     cache,
		opts:      opts,
		compactCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		stats:     &CompactionStats{},
	}
	l.stallCond = sync.NewCond(&l.mu)

	var rateLimiter *RateLimiter
	if opts.CompactionRateMB > 0 {
		rateLimiter = NewRateLimiter(int64(opts.CompactionRateMB) * 1024 * 1024)
	}

	l.compactor = NewCompactionExecutor(opts.Dir, manifest, cache, opts.BlockSize, opts.BloomBitsPerKey, TargetSize(1), rateLimiter, opts.MinActiveSnapshot)

	// Load existing SSTables
	version := manifest.Current()
	for i := 0; i < MaxLevels; i++ {
		for _, f := range version.Levels[i] {
			path := filepath.Join(opts.Dir, fmt.Sprintf("%06d.sst", f.FileNum))
			r, err := sstable.OpenReader(path, f.FileNum, cache)
			if err != nil {
				return nil, err
			}
			l.readers[f.FileNum] = r
		}
	}

	return l, nil
}

// Stats returns a snapshot of the current compaction statistics.
func (l *LSMTree) Stats() CompactionStatsSnapshot {
	return l.stats.Snapshot()
}

// Flush writes a MemTable to L0 as a new SSTable.
func (l *LSMTree) Flush(mt *memtable.MemTable) error {
	l.WaitForStall()

	fileNum := l.manifest.NextFileNumber()
	path := filepath.Join(l.dir, fmt.Sprintf("%06d.sst", fileNum))

	w, err := sstable.NewWriter(path, l.opts.BlockSize, l.opts.BloomBitsPerKey)
	if err != nil {
		return err
	}

	var smallest, largest []byte
	var numEntries uint64
	it := mt.NewIterator()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if numEntries == 0 {
			smallest = append([]byte(nil), it.Key()...)
		}
		largest = append([]byte(nil), it.Key()...)

		if err := w.Add(it.Key(), it.Value()); err != nil {
			w.Close()
			return err
		}
		numEntries++
	}

	if err := w.Close(); err != nil {
		return err
	}

	stat, err := os.Stat(path)
	if err != nil {
		return err
	}

	r, err := sstable.OpenReader(path, fileNum, l.cache)
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.readers[fileNum] = r
	l.mu.Unlock()

	edit := &VersionEdit{
		AddedFiles: []FileMetadata{
			{
				Level:       0,
				FileNum:     fileNum,
				FileSize:    uint64(stat.Size()),
				SmallestKey: smallest,
				LargestKey:  largest,
				NumEntries:  numEntries,
			},
		},
	}

	if err := l.manifest.Apply(edit); err != nil {
		return err
	}

	l.CheckWriteStall()

	// Trigger compaction check
	select {
	case l.compactCh <- struct{}{}:
	default:
	}

	return nil
}

// Get performs a point lookup across all levels.
func (l *LSMTree) Get(key []byte) ([]byte, bool, error) {
	version := l.manifest.Current()

	l.mu.RLock()
	defer l.mu.RUnlock()

	// L0: scan all files newest-first
	l0Files := version.Levels[0]
	for i := len(l0Files) - 1; i >= 0; i-- {
		f := l0Files[i]
		if r, ok := l.readers[f.FileNum]; ok {
			val, found, err := r.Get(key)
			if err != nil {
				return nil, false, err
			}
			if found {
				return val, true, nil
			}
		}
	}

	// L1+: binary search for the single file containing key
	for level := 1; level < MaxLevels; level++ {
		files := version.Levels[level]
		if len(files) == 0 {
			continue
		}

		// Since files in L1+ are non-overlapping and sorted, we can binary search
		left, right := 0, len(files)-1
		var target *FileMetadata
		for left <= right {
			mid := left + (right-left)/2
			f := files[mid]

			if mvcc.CompareKeys(key, f.SmallestKey) >= 0 && mvcc.CompareKeys(key, f.LargestKey) <= 0 {
				target = &f
				break
			} else if mvcc.CompareKeys(key, f.SmallestKey) < 0 {
				right = mid - 1
			} else {
				left = mid + 1
			}
		}

		if target != nil {
			if r, ok := l.readers[target.FileNum]; ok {
				val, found, err := r.Get(key)
				if err != nil {
					return nil, false, err
				}
				if found {
					return val, true, nil
				}
			}
		}
	}

	return nil, false, nil
}

// StartCompaction launches background compaction goroutines.
func (l *LSMTree) StartCompaction() {
	l.compactWg.Add(1)
	go l.compactionLoop()
}

func (l *LSMTree) compactionLoop() {
	defer l.compactWg.Done()

	for {
		select {
		case <-l.compactCh:
			l.doCompaction()
		case <-l.stopCh:
			return
		}
	}
}

func (l *LSMTree) doCompaction() {
	l.stats.IncrPending()
	defer l.stats.DecrPending()

	for {
		version := l.manifest.Current()
		picker := &CompactionPicker{version: version}
		c := picker.PickCompaction()
		if c == nil {
			break // No more compactions needed
		}

		maxOccupiedLevel := 0
		for i := 0; i < MaxLevels; i++ {
			if len(version.Levels[i]) > 0 {
				maxOccupiedLevel = i
			}
		}

		start := time.Now()
		bytesIn, bytesOut, err := l.compactor.Execute(c, maxOccupiedLevel)
		if err != nil {
			log.Printf("Compaction failed: %v", err)
			time.Sleep(1 * time.Second) // backoff
			continue
		}

		l.stats.RecordCompaction(bytesIn, bytesOut, time.Since(start))

		// Clean up old readers
		l.mu.Lock()
		for i := 0; i < 2; i++ {
			for _, f := range c.InputFiles[i] {
				if r, ok := l.readers[f.FileNum]; ok {
					r.Close()
					delete(l.readers, f.FileNum)
				}
			}
		}

		// Add new readers
		version = l.manifest.Current()
		for _, f := range version.Levels[c.OutputLevel] {
			if _, ok := l.readers[f.FileNum]; !ok {
				path := filepath.Join(l.dir, fmt.Sprintf("%06d.sst", f.FileNum))
				r, err := sstable.OpenReader(path, f.FileNum, l.cache)
				if err == nil {
					l.readers[f.FileNum] = r
				} else {
					log.Printf("Failed to open compacted file %s: %v", path, err)
				}
			}
		}
		l.mu.Unlock()

		l.ReleaseStall()
	}
}

// Close stops compaction and closes all readers.
func (l *LSMTree) Close() error {
	close(l.stopCh)
	l.compactWg.Wait()

	if l.compactor.rateLimiter != nil {
		l.compactor.rateLimiter.Stop()
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, r := range l.readers {
		r.Close()
	}

	return l.manifest.Close()
}
