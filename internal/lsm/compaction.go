package lsm

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"

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
	var bestLevel int = -1
	var bestScore float64 = -1.0

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
				if bytes.Compare(f.SmallestKey, smallest) < 0 {
					smallest = f.SmallestKey
				}
				if bytes.Compare(f.LargestKey, largest) > 0 {
					largest = f.LargestKey
				}
			}
		}
		c.InputFiles[1] = GetOverlappingFiles(p.version.Levels[1], smallest, largest)
	} else {
		// Pick one file from bestLevel (e.g., the largest or oldest)
		// Here we just pick the first one for simplicity
		c.InputFiles[0] = []FileMetadata{p.version.Levels[bestLevel][0]}
		smallest := c.InputFiles[0][0].SmallestKey
		largest := c.InputFiles[0][0].LargestKey
		
		c.InputFiles[1] = GetOverlappingFiles(p.version.Levels[bestLevel+1], smallest, largest)
	}

	return c
}

// CompactionExecutor performs the multi-way merge.
type CompactionExecutor struct {
	dbDir       string
	manifest    *Manifest
	cache       *sstable.BlockCache
	blockSize   int
	bloomBits   int
	targetSize  uint64
	rateLimiter *RateLimiter
}

func NewCompactionExecutor(dbDir string, manifest *Manifest, cache *sstable.BlockCache, blockSize, bloomBits int, targetSize uint64, rateLimiter *RateLimiter) *CompactionExecutor {
	return &CompactionExecutor{
		dbDir:       dbDir,
		manifest:    manifest,
		cache:       cache,
		blockSize:   blockSize,
		bloomBits:   bloomBits,
		targetSize:  targetSize,
		rateLimiter: rateLimiter,
	}
}

// Execute performs the compaction and applies it to the manifest.
func (e *CompactionExecutor) Execute(c *Compaction) error {
	var iters []Iterator
	var readers []*sstable.Reader

	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	// Open readers for all input files
	for i := 0; i < 2; i++ {
		for _, f := range c.InputFiles[i] {
			path := filepath.Join(e.dbDir, fmt.Sprintf("%06d.sst", f.FileNum))
			r, err := sstable.OpenReader(path, f.FileNum, e.cache)
			if err != nil {
				return err
			}
			readers = append(readers, r)
			iters = append(iters, r.NewIterator())
		}
	}

	mergeIter := NewMergeIterator(iters...)

	var currentWriter *sstable.Writer
	var currentFileNum uint64
	var currentSmallest []byte
	var addedFiles []FileMetadata
	var entriesWritten uint64
	var currentSize uint64

	finishCurrentFile := func() error {
		if currentWriter != nil {
			if err := currentWriter.Close(); err != nil {
				return err
			}
			
			// Get file size
			path := filepath.Join(e.dbDir, fmt.Sprintf("%06d.sst", currentFileNum))
			stat, err := os.Stat(path)
			if err != nil {
				return err
			}

			// Using a copy of the last key we processed as largest
			// The actual writer doesn't easily expose largest key directly after close without an accessor
			// We track it during writes
			
			addedFiles = append(addedFiles, FileMetadata{
				Level:       c.OutputLevel,
				FileNum:     currentFileNum,
				FileSize:    uint64(stat.Size()),
				SmallestKey: currentSmallest,
				// LargestKey will be filled below
				NumEntries:  entriesWritten,
			})
			currentWriter = nil
		}
		return nil
	}

	var lastKey []byte

	for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
		key := mergeIter.Key()
		val := mergeIter.Value()

		// Skip tombstones if this key doesn't exist in deeper levels
		// For week 3 simplicity, we just keep tombstones unless it's the deepest level
		// Actually, let's keep all tombstones for now to avoid accidental data resurrection

		if currentWriter == nil {
			currentFileNum = e.manifest.NextFileNumber()
			path := filepath.Join(e.dbDir, fmt.Sprintf("%06d.sst", currentFileNum))
			w, err := sstable.NewWriter(path, e.blockSize, e.bloomBits)
			if err != nil {
				return err
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
			return err
		}
		entriesWritten++
		currentSize += uint64(len(key) + len(val) + 8) // rough estimate
		lastKey = append(lastKey[:0], key...)

		if currentSize >= e.targetSize {
			if err := finishCurrentFile(); err != nil {
				return err
			}
			// Set largest key for the finished file
			addedFiles[len(addedFiles)-1].LargestKey = append([]byte(nil), lastKey...)
		}
	}

	if err := finishCurrentFile(); err != nil {
		return err
	}
	if len(addedFiles) > 0 {
		addedFiles[len(addedFiles)-1].LargestKey = append([]byte(nil), lastKey...)
	}

	// Prepare VersionEdit
	edit := &VersionEdit{
		AddedFiles: addedFiles,
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
		return err
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

	return nil
}
