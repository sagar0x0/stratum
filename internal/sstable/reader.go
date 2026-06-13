package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sort"

	"github.com/pierrec/lz4/v4"
	"github.com/sagar0x0/stratum/internal/bloom"
)

var (
	ErrNotFound = errors.New("sstable: key not found")
	ErrCorrupt  = errors.New("sstable: file corrupt")
)

// Reader is used to read an SSTable.
type Reader struct {
	file         *os.File
	FileID       uint64
	footer       Footer
	indexEntries []IndexEntry
	bloom        *bloom.Filter
	cache        *BlockCache
	fileSize     int64
}

// OpenReader opens an SSTable file for reading.
func OpenReader(path string, fileID uint64, cache *BlockCache) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	fileSize := stat.Size()

	if fileSize < FooterSize {
		file.Close()
		return nil, ErrCorrupt
	}

	// Read Footer
	footerBuf := make([]byte, FooterSize)
	if _, err := file.ReadAt(footerBuf, fileSize-FooterSize); err != nil {
		file.Close()
		return nil, err
	}

	footer, ok := DecodeFooter(footerBuf)
	if !ok {
		file.Close()
		return nil, ErrCorrupt
	}

	// Read Index Block
	indexBuf := make([]byte, footer.IndexHandle.Size)
	if _, err := file.ReadAt(indexBuf, int64(footer.IndexHandle.Offset)); err != nil {
		file.Close()
		return nil, err
	}

	var indexEntries []IndexEntry
	offset := 0
	for offset < len(indexBuf) {
		if offset+4 > len(indexBuf) {
			file.Close()
			return nil, ErrCorrupt
		}
		keyLen := int(binary.LittleEndian.Uint32(indexBuf[offset : offset+4]))
		offset += 4

		if offset+keyLen+16 > len(indexBuf) {
			file.Close()
			return nil, ErrCorrupt
		}
		key := indexBuf[offset : offset+keyLen]
		offset += keyLen

		handle := DecodeBlockHandle(indexBuf[offset : offset+16])
		offset += 16

		indexEntries = append(indexEntries, IndexEntry{
			Key:    key,
			Handle: handle,
		})
	}

	// Read Bloom Filter Block
	bloomBuf := make([]byte, footer.BloomHandle.Size)
	if _, err := file.ReadAt(bloomBuf, int64(footer.BloomHandle.Offset)); err != nil {
		file.Close()
		return nil, err
	}

	bf := bloom.DecodeFilter(bloomBuf)

	return &Reader{
		file:         file,
		FileID:       fileID,
		footer:       footer,
		indexEntries: indexEntries,
		bloom:        bf,
		cache:        cache,
		fileSize:     fileSize,
	}, nil
}

// readBlock reads and decompresses a data block.
func (r *Reader) readBlock(handle BlockHandle) ([]byte, error) {
	if r.cache != nil {
		if data, ok := r.cache.Get(r.FileID, handle.Offset); ok {
			return data, nil
		}
	}

	buf := make([]byte, handle.Size)
	if _, err := r.file.ReadAt(buf, int64(handle.Offset)); err != nil {
		return nil, err
	}

	if len(buf) < 1 {
		return nil, ErrCorrupt
	}

	isCompressed := buf[0] == 1
	var blockData []byte

	if isCompressed {
		// Use a sufficiently large buffer for decompression.
		// A common approach is to prepend uncompressed length, but since we didn't,
		// we can guess or use a growing buffer. Let's start with 8x block size.
		dest := make([]byte, 8*DefaultBlockSize)
		n, err := lz4.UncompressBlock(buf[1:], dest)
		if err != nil {
			return nil, err
		}
		blockData = dest[:n]
	} else {
		blockData = buf[1:]
	}

	if r.cache != nil {
		// We copy the data to ensure it's not sharing backing array if we over-allocated
		cachedData := make([]byte, len(blockData))
		copy(cachedData, blockData)
		r.cache.Put(r.FileID, handle.Offset, cachedData)
		blockData = cachedData
	}

	return blockData, nil
}

// findBlockIndex finds the index of the data block that could contain the key
func (r *Reader) findBlockIndex(key []byte) int {
	return sort.Search(len(r.indexEntries), func(i int) bool {
		return bytes.Compare(r.indexEntries[i].Key, key) >= 0
	})
}

// Get returns the value for the given key, and a boolean indicating if it was found.
// It returns true, nil for tombstones (key found, but deleted).
// Returns false, nil if key is not found at all.
func (r *Reader) Get(key []byte) ([]byte, bool, error) {
	if !r.bloom.MayContain(key) {
		return nil, false, nil
	}

	idx := r.findBlockIndex(key)
	if idx >= len(r.indexEntries) {
		return nil, false, nil
	}

	handle := r.indexEntries[idx].Handle
	blockData, err := r.readBlock(handle)
	if err != nil {
		return nil, false, err
	}

	// Scan through block
	offset := 0
	for offset < len(blockData) {
		if offset+8 > len(blockData) {
			return nil, false, ErrCorrupt
		}
		keyLen := int(binary.LittleEndian.Uint32(blockData[offset : offset+4]))
		valLenRaw := binary.LittleEndian.Uint32(blockData[offset+4 : offset+8])
		offset += 8

		isTombstone := valLenRaw == 0xFFFFFFFF
		var valLen int
		if !isTombstone {
			valLen = int(valLenRaw)
		}

		if offset+keyLen+valLen > len(blockData) {
			return nil, false, ErrCorrupt
		}

		currentKey := blockData[offset : offset+keyLen]
		offset += keyLen

		var currentVal []byte
		if !isTombstone {
			currentVal = blockData[offset : offset+valLen]
			offset += valLen
		}

		cmp := bytes.Compare(currentKey, key)
		if cmp == 0 {
			if isTombstone {
				return nil, true, nil
			}
			// Copy value to prevent memory leaks from the block buffer
			valCopy := make([]byte, len(currentVal))
			copy(valCopy, currentVal)
			return valCopy, true, nil
		} else if cmp > 0 {
			// Because block is sorted, if we see a key > target, it's not here
			break
		}
	}

	return nil, false, nil
}

// NewIterator creates a new iterator over the SSTable.
func (r *Reader) NewIterator() *Iterator {
	it := &Iterator{
		reader: r,
	}
	it.SeekToFirst()
	return it
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	return r.file.Close()
}
