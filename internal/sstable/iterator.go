package sstable

import (
	"bytes"
	"encoding/binary"
)

// Iterator iterates over all key-value pairs in an SSTable.
type Iterator struct {
	reader   *Reader
	blockIdx int
	offset   int
	err      error

	blockData   []byte
	currentKey  []byte
	currentVal  []byte
	isTombstone bool
	valid       bool
}

// SeekToFirst moves the iterator to the first key in the SSTable.
func (it *Iterator) SeekToFirst() {
	it.blockIdx = 0
	it.loadBlock()
}

// Seek moves the iterator to the first key >= target.
func (it *Iterator) Seek(target []byte) {
	it.blockIdx = it.reader.findBlockIndex(target)
	it.loadBlock()

	for it.Valid() {
		if bytes.Compare(it.Key(), target) >= 0 {
			break
		}
		it.Next()
	}
}

// Next moves to the next key-value pair.
func (it *Iterator) Next() {
	if !it.valid {
		return
	}

	if it.offset >= len(it.blockData) {
		it.blockIdx++
		it.loadBlock()
		return
	}

	it.parseCurrent()
}

func (it *Iterator) loadBlock() {
	it.valid = false
	if it.blockIdx >= len(it.reader.indexEntries) {
		return
	}

	handle := it.reader.indexEntries[it.blockIdx].Handle
	data, err := it.reader.readBlock(handle)
	if err != nil {
		it.err = err
		return
	}

	it.blockData = data
	it.offset = 0
	it.parseCurrent()
}

func (it *Iterator) parseCurrent() {
	if it.offset >= len(it.blockData) {
		it.blockIdx++
		it.loadBlock()
		return
	}

	if it.offset+8 > len(it.blockData) {
		it.err = ErrCorrupt
		it.valid = false
		return
	}

	keyLen := int(binary.LittleEndian.Uint32(it.blockData[it.offset : it.offset+4]))
	valLenRaw := binary.LittleEndian.Uint32(it.blockData[it.offset+4 : it.offset+8])
	it.offset += 8

	it.isTombstone = valLenRaw == 0xFFFFFFFF
	var valLen int
	if !it.isTombstone {
		valLen = int(valLenRaw)
	}

	if it.offset+keyLen+valLen > len(it.blockData) {
		it.err = ErrCorrupt
		it.valid = false
		return
	}

	it.currentKey = it.blockData[it.offset : it.offset+keyLen]
	it.offset += keyLen

	if !it.isTombstone {
		it.currentVal = it.blockData[it.offset : it.offset+valLen]
		it.offset += valLen
	} else {
		it.currentVal = nil
	}

	it.valid = true
}

// Valid returns true if the iterator is currently pointing to a valid entry.
func (it *Iterator) Valid() bool {
	return it.valid && it.err == nil
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	return it.currentKey
}

// Value returns the current value. It returns nil for tombstones.
func (it *Iterator) Value() []byte {
	return it.currentVal
}

// IsTombstone returns true if the current entry is a tombstone.
func (it *Iterator) IsTombstone() bool {
	return it.isTombstone
}

// Err returns any error encountered during iteration.
func (it *Iterator) Err() error {
	return it.err
}
