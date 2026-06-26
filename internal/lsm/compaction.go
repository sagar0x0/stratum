package lsm

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/sagar0x0/stratum/internal/mvcc"
	"github.com/sagar0x0/stratum/internal/sstable"
)

// Compaction represents a pending compaction operation.
type Compaction struct {
	Level       int               // Source level
	InputFiles  [2][]FileMetadata // [0] = source level files, [1] = target level overlapping files
	OutputLevel int
}

// CompactionPicker selects the next compaction to perform.
type CompactionPicker struct {
	version *Version
}

// PickCompaction returns the next compaction to execute, or nil if none needed.
func (p *CompactionPicker) PickCompaction() *Compaction {
	// Calculate scores for each level
	var bestLevel = -1
	var bestScore = -1.0

	for level := 0; level < MaxLevels-1; level++ {
		score := CompactionScore(level, p.version.Levels[level])
		if score > bestScore {
			bestScore = score
			bestLevel = level
		}
	}

	if bestScore <= 1.0 {
		return nil // No compaction needed
	}

	c := &Compaction{
		Level:       bestLevel,
		OutputLevel: bestLevel + 1,
	}

	if bestLevel == 0 {
		// For L0, we must compact all files that overlap.
		// To be safe, we just take all L0 files.
		c.InputFiles[0] = append([]FileMetadata(nil), p.version.Levels[0]...)

		// Find overlapping L1 files
		var smallest, largest []byte
		for i, f := range c.InputFiles[0] {
			if i == 0 {
				smallest = f.SmallestKey
				largest = f.LargestKey
			} else {
				if mvcc.CompareKeys(f.SmallestKey, smallest) < 0 {
					smallest = f.SmallestKey
				}
				if mvcc.CompareKeys(f.LargestKey, largest) > 0 {
					largest = f.LargestKey
				}
			}
		}
		c.InputFiles[1] = GetOverlappingFiles(p.version.Levels[1], smallest, largest)
	} else {
		// Pick the largest file in the level to maximize compaction impact
		largestFile := p.version.Levels[bestLevel][0]
		for _, f := range p.version.Levels[bestLevel][1:] {
			if f.FileSize > largestFile.FileSize {
				largestFile = f
			}
		}
		c.InputFiles[0] = []FileMetadata{largestFile}
		smallest := c.InputFiles[0][0].SmallestKey
		largest := c.InputFiles[0][0].LargestKey

		c.InputFiles[1] = GetOverlappingFiles(p.version.Levels[bestLevel+1], smallest, largest)
	}

	return c
}

// CompactionExecutor performs the multi-way merge.
type CompactionExecutor struct {
	dbDir             string
	manifest          *Manifest
	cache             *sstable.BlockCache
	blockSize         int
	bloomBits         int
	targetSize        uint64
	rateLimiter       *RateLimiter
	minActiveSnapshot func() uint64
}

func NewCompactionExecutor(dbDir string, manifest *Manifest, cache *sstable.BlockCache, blockSize, bloomBits int, targetSize uint64, rateLimiter *RateLimiter, minActiveSnapshot func() uint64) *CompactionExecutor {
	return &CompactionExecutor{
		dbDir:             dbDir,
		manifest:          manifest,
		cache:             cache,
		blockSize:         blockSize,
		bloomBits:         bloomBits,
		targetSize:        targetSize,
		rateLimiter:       rateLimiter,
		minActiveSnapshot: minActiveSnapshot,
	}
}

// Execute runs a compaction and updates the manifest.
// Returns (bytesIn, bytesOut, error).
func (e *CompactionExecutor) Execute(c *Compaction, maxOccupiedLevel int) (int64, int64, error) {
	if c == nil || len(c.InputFiles[0]) == 0 {
		return 0, 0, nil
	}
	var readers []*sstable.Reader

	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	// Open readers for all input files
	var iters []Iterator
	for i := 0; i < 2; i++ {
		for _, f := range c.InputFiles[i] {
			path := filepath.Join(e.dbDir, fmt.Sprintf("%06d.sst", f.FileNum))
			r, err := sstable.OpenReader(path, f.FileNum, e.cache)
			if err != nil {
				return 0, 0, err
			}
			readers = append(readers, r)
			iters = append(iters, r.NewIterator())
		}
	}

	var totalBytesIn, totalBytesOut int64
	for _, f := range c.InputFiles[0] {
		totalBytesIn += int64(f.FileSize)
	}
	for _, f := range c.InputFiles[1] {
		totalBytesIn += int64(f.FileSize)
	}

	mergeIter := NewMergeIterator(iters...)

	var addedFiles []FileMetadata
	var currentWriter *sstable.Writer
	var currentFileNum uint64
	var currentSmallest []byte
	var entriesWritten uint64
	var currentSize uint64

	finishCurrentFile := func() error {
		if currentWriter != nil {
			if err := currentWriter.Close(); err != nil {
				return err
			}
			fileSize := currentWriter.Size()
			totalBytesOut += int64(fileSize)

			addedFiles = append(addedFiles, FileMetadata{
				Level:       c.OutputLevel,
				FileNum:     currentFileNum,
				FileSize:    fileSize,
				SmallestKey: currentSmallest,
				NumEntries:  entriesWritten,
			})
			currentWriter = nil
		}
		return nil
	}

	var lastKey []byte
	var lastUserKey []byte
	var seenOlderThanMinActive bool

	for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
		key := mergeIter.Key()
		val := mergeIter.Value()

		var minActive uint64 = 0
		if e.minActiveSnapshot != nil {
			minActive = e.minActiveSnapshot()
		}

		userKey, ts := mvcc.DecodeKey(key)

		if !bytes.Equal(userKey, lastUserKey) {
			lastUserKey = append(lastUserKey[:0], userKey...)
			seenOlderThanMinActive = false
		}

		if ts < minActive {
			if seenOlderThanMinActive {
				// We already kept a version older than minActive for this user key.
				// This version is even older, so no active txn can ever read it.
				continue
			}
			seenOlderThanMinActive = true

			// Drop tombstones at the deepest occupied level -
			// no older version can exist below, and no active txn needs this tombstone.
			if val == nil && c.OutputLevel >= maxOccupiedLevel {
				continue
			}
		}

		if currentWriter == nil {
			currentFileNum = e.manifest.NextFileNumber()
			path := filepath.Join(e.dbDir, fmt.Sprintf("%06d.sst", currentFileNum))
			w, err := sstable.NewWriter(path, e.blockSize, e.bloomBits)
			if err != nil {
				return 0, 0, err
			}
			currentWriter = w
			currentSmallest = append([]byte(nil), key...)
			entriesWritten = 0
			currentSize = 0
		}

		if e.rateLimiter != nil {
			e.rateLimiter.Request(int64(len(key) + len(val) + 8))
		}

		if err := currentWriter.Add(key, val); err != nil {
			return 0, 0, err
		}
		entriesWritten++
		currentSize += uint64(len(key) + len(val) + 8) // rough estimate
		lastKey = append(lastKey[:0], key...)

		if currentSize >= e.targetSize {
			if err := finishCurrentFile(); err != nil {
				return 0, 0, err
			}
			// Set largest key for the finished file
			addedFiles[len(addedFiles)-1].LargestKey = append([]byte(nil), lastKey...)
		}
	}

	if err := finishCurrentFile(); err != nil {
		return 0, 0, err
	}
	if len(addedFiles) > 0 {
		addedFiles[len(addedFiles)-1].LargestKey = append([]byte(nil), lastKey...)
	}

	// Prepare VersionEdit
	edit := &VersionEdit{
		AddedFiles:   addedFiles,
		DeletedFiles: make([]DeletedFile, 0),
	}

	for i := 0; i < 2; i++ {
		for _, f := range c.InputFiles[i] {
			edit.DeletedFiles = append(edit.DeletedFiles, DeletedFile{
				Level:   c.Level + i, // input[0] is source level, input[1] is output level
				FileNum: f.FileNum,
			})
		}
	}

	if err := e.manifest.Apply(edit); err != nil {
		return 0, 0, err
	}

	// Delete old files from disk
	for _, df := range edit.DeletedFiles {
		if e.cache != nil {
			e.cache.RemoveFile(df.FileNum)
		}
		path := filepath.Join(e.dbDir, fmt.Sprintf("%06d.sst", df.FileNum))
		if err := os.Remove(path); err != nil {
			log.Printf("Failed to delete compacted file %s: %v", path, err)
		}
	}

	return totalBytesIn, totalBytesOut, nil
}
