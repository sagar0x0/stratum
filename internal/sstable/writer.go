package sstable

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/pierrec/lz4/v4"
	"github.com/sagar0x0/stratum/internal/bloom"
)

// IndexEntry represents a pointer to a data block
type IndexEntry struct {
	Key    []byte // The last key in the data block
	Handle BlockHandle
}

// Writer creates an SSTable file
type Writer struct {
	file         *os.File
	blockBuf     bytes.Buffer // current data block accumulator
	indexEntries []IndexEntry
	bloom        *bloom.Filter
	offset       uint64 // current file write offset
	blockSize    int
	entryCount   uint64
	lastKey      []byte
	lz4Buf       []byte // reusable buffer for lz4 compression
}

// NewWriter creates a new SSTable writer
func NewWriter(path string, blockSize int, bloomBitsPerKey int) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	// We'll initialize the bloom filter with a guess, e.g., 10000 keys.
	// In a real system, we might know the exact number of keys from the MemTable.
	// Here we just use a default or dynamically resize. Wait, bloom filters can't be resized easily.
	// For now, let's assume 10000.
	bf := bloom.NewFilter(10000, bloomBitsPerKey)

	return &Writer{
		file:      file,
		bloom:     bf,
		blockSize: blockSize,
		lz4Buf:    make([]byte, lz4.CompressBlockBound(blockSize+1024)), // some padding
	}, nil
}

// Add adds a key-value pair to the SSTable. Keys must be added in sorted order.
func (w *Writer) Add(key, value []byte) error {
	w.bloom.Add(key)

	// Format: [keyLen(4)][valLen(4)][key][val]
	// where valLen is uint32 max for tombstone if value == nil
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	if value == nil {
		binary.LittleEndian.PutUint32(header[4:8], 0xFFFFFFFF)
	} else {
		binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
	}

	w.blockBuf.Write(header[:])
	w.blockBuf.Write(key)
	if value != nil {
		w.blockBuf.Write(value)
	}

	w.entryCount++
	w.lastKey = append(w.lastKey[:0], key...)

	if w.blockBuf.Len() >= w.blockSize {
		return w.flushDataBlock()
	}

	return nil
}

func (w *Writer) flushDataBlock() error {
	if w.blockBuf.Len() == 0 {
		return nil
	}

	uncompressed := w.blockBuf.Bytes()
	
	// Compress with LZ4
	// Ensure lz4Buf is large enough
	bound := lz4.CompressBlockBound(len(uncompressed))
	if len(w.lz4Buf) < bound {
		w.lz4Buf = make([]byte, bound)
	}

	var compressedSize int
	var err error
	
	var compressor lz4.Compressor
	compressedSize, err = compressor.CompressBlock(uncompressed, w.lz4Buf)
	if err != nil {
		return err
	}

	var dataToWrite []byte
	var isCompressed bool

	if compressedSize > 0 && compressedSize < len(uncompressed) {
		dataToWrite = w.lz4Buf[:compressedSize]
		isCompressed = true
	} else {
		dataToWrite = uncompressed
		isCompressed = false
	}

	// Format of block on disk: [isCompressed(1)][compressedSize(4)][data]
	// Wait, to keep it simple, let's just write [isCompressed(1)][data] and we know the size from the BlockHandle
	// Oh, BlockHandle size is the total size.
	// So we can write [isCompressed(1)] followed by data.
	
	blockHeader := []byte{0}
	if isCompressed {
		blockHeader[0] = 1
	}

	if _, err := w.file.Write(blockHeader); err != nil {
		return err
	}
	
	n, err := w.file.Write(dataToWrite)
	if err != nil {
		return err
	}

	blockSize := uint64(1 + n)
	
	// Record index entry
	handle := BlockHandle{
		Offset: w.offset,
		Size:   blockSize,
	}
	
	lastK := make([]byte, len(w.lastKey))
	copy(lastK, w.lastKey)
	
	w.indexEntries = append(w.indexEntries, IndexEntry{
		Key:    lastK,
		Handle: handle,
	})

	w.offset += blockSize
	w.blockBuf.Reset()

	return nil
}

// Close finishes the SSTable by flushing the remaining data, writing the index,
// bloom filter, and footer, and closing the file.
func (w *Writer) Close() error {
	if err := w.flushDataBlock(); err != nil {
		w.file.Close()
		return err
	}

	// Write Index Block
	indexOffset := w.offset
	var indexBuf bytes.Buffer
	for _, ie := range w.indexEntries {
		var header [4]byte
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(ie.Key)))
		indexBuf.Write(header[:])
		indexBuf.Write(ie.Key)
		indexBuf.Write(ie.Handle.Encode())
	}
	
	n, err := w.file.Write(indexBuf.Bytes())
	if err != nil {
		w.file.Close()
		return err
	}
	indexSize := uint64(n)
	w.offset += indexSize

	// Write Bloom Block
	bloomOffset := w.offset
	bloomData := w.bloom.Encode()
	n, err = w.file.Write(bloomData)
	if err != nil {
		w.file.Close()
		return err
	}
	bloomSize := uint64(n)
	w.offset += bloomSize

	// Write Footer
	footer := Footer{
		IndexHandle: BlockHandle{Offset: indexOffset, Size: indexSize},
		BloomHandle: BlockHandle{Offset: bloomOffset, Size: bloomSize},
		NumEntries:  w.entryCount,
		Magic:       MagicNumber,
	}
	
	if _, err := w.file.Write(footer.Encode()); err != nil {
		w.file.Close()
		return err
	}

	return w.file.Close()
}
