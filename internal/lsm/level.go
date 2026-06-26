package lsm

import (
	"sort"

	"github.com/sagar0x0/stratum/internal/mvcc"
)

const (
	L0CompactionTrigger = 4
	LevelSizeRatio      = 10
	BaseLevelSize       = 10 * 1024 * 1024 // 10 MB
)

// GetOverlappingFiles returns files in a level whose key range overlaps [smallest, largest].
func GetOverlappingFiles(files []FileMetadata, smallest, largest []byte) []FileMetadata {
	var overlapping []FileMetadata
	for _, f := range files {
		if mvcc.CompareKeys(f.LargestKey, smallest) >= 0 && mvcc.CompareKeys(f.SmallestKey, largest) <= 0 {
			overlapping = append(overlapping, f)
		}
	}
	return overlapping
}

// SortBySmallestKey sorts files by their smallest key.
func SortBySmallestKey(files []FileMetadata) {
	sort.Slice(files, func(i, j int) bool {
		return mvcc.CompareKeys(files[i].SmallestKey, files[j].SmallestKey) < 0
	})
}

// TargetSize returns the target maximum size for a given level.
func TargetSize(level int) uint64 {
	if level <= 0 {
		return 0 // L0 doesn't have a strict target size, it's triggered by file count
	}
	size := uint64(BaseLevelSize)
	for i := 1; i < level; i++ {
		size *= LevelSizeRatio
	}
	return size
}

// CompactionScore calculates priority for compaction.
// Returns a score where > 1.0 means compaction is needed.
func CompactionScore(level int, files []FileMetadata) float64 {
	if level == 0 {
		return float64(len(files)) / float64(L0CompactionTrigger)
	}

	var totalSize uint64
	for _, f := range files {
		totalSize += f.FileSize
	}

	target := TargetSize(level)
	return float64(totalSize) / float64(target)
}
