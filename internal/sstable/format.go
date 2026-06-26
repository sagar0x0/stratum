package sstable

import (
	"encoding/binary"
)

const (
	DefaultBlockSize = 4096               // 4 KB data blocks
	FooterSize       = 48                 // Fixed footer size
	MagicNumber      = 0x5354524154554D00 // "STRATUM\0"
)

// BlockHandle locates a block within the SSTable file
type BlockHandle struct {
	Offset uint64
	Size   uint64
}

// Encode writes the BlockHandle to a byte slice
func (h *BlockHandle) Encode() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], h.Offset)
	binary.LittleEndian.PutUint64(buf[8:16], h.Size)
	return buf
}

// DecodeBlockHandle reads a BlockHandle from a byte slice
func DecodeBlockHandle(data []byte) BlockHandle {
	if len(data) < 16 {
		return BlockHandle{}
	}
	return BlockHandle{
		Offset: binary.LittleEndian.Uint64(data[0:8]),
		Size:   binary.LittleEndian.Uint64(data[8:16]),
	}
}

// Footer is the final 48 bytes of every SSTable
type Footer struct {
	IndexHandle BlockHandle // 16 bytes
	BloomHandle BlockHandle // 16 bytes
	NumEntries  uint64      // 8 bytes
	Magic       uint64      // 8 bytes
}

// Encode writes the Footer to a byte slice
func (f *Footer) Encode() []byte {
	buf := make([]byte, FooterSize)
	copy(buf[0:16], f.IndexHandle.Encode())
	copy(buf[16:32], f.BloomHandle.Encode())
	binary.LittleEndian.PutUint64(buf[32:40], f.NumEntries)
	binary.LittleEndian.PutUint64(buf[40:48], f.Magic)
	return buf
}

// DecodeFooter reads a Footer from a byte slice
func DecodeFooter(data []byte) (Footer, bool) {
	if len(data) < FooterSize {
		return Footer{}, false
	}
	magic := binary.LittleEndian.Uint64(data[40:48])
	if magic != MagicNumber {
		return Footer{}, false
	}
	return Footer{
		IndexHandle: DecodeBlockHandle(data[0:16]),
		BloomHandle: DecodeBlockHandle(data[16:32]),
		NumEntries:  binary.LittleEndian.Uint64(data[32:40]),
		Magic:       magic,
	}, true
}

// Entry represents a single key-value pair in a data block
type Entry struct {
	Key   []byte
	Value []byte // nil represents a tombstone
}
