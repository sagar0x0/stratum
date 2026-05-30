package wal

import (
	"encoding/binary"
	"errors"
)

type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

type BatchEntry struct {
	Op    OpType
	Key   []byte
	Value []byte // nil for Delete
}

type Batch struct {
	Entries []BatchEntry
}

func (b *Batch) Put(key, value []byte) {
	b.Entries = append(b.Entries, BatchEntry{Op: OpPut, Key: key, Value: value})
}

func (b *Batch) Delete(key []byte) {
	b.Entries = append(b.Entries, BatchEntry{Op: OpDelete, Key: key})
}

func (b *Batch) Encode() []byte {
	size := 4
	for _, e := range b.Entries {
		size += 1 + 4 + len(e.Key) + 4 + len(e.Value)
	}

	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(b.Entries)))
	offset := 4

	for _, e := range b.Entries {
		buf[offset] = byte(e.Op)
		offset += 1

		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(e.Key)))
		offset += 4
		copy(buf[offset:offset+len(e.Key)], e.Key)
		offset += len(e.Key)

		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(e.Value)))
		offset += 4
		copy(buf[offset:offset+len(e.Value)], e.Value)
		offset += len(e.Value)
	}
	return buf
}

func DecodeBatch(data []byte) (*Batch, error) {
	if len(data) < 4 {
		return nil, errors.New("wal: invalid batch format")
	}

	count := int(binary.LittleEndian.Uint32(data[0:4]))
	b := &Batch{Entries: make([]BatchEntry, 0, count)}
	offset := 4

	for i := 0; i < count; i++ {
		if offset+1 > len(data) {
			return nil, errors.New("wal: invalid batch format")
		}
		op := OpType(data[offset])
		offset += 1

		if offset+4 > len(data) {
			return nil, errors.New("wal: invalid batch format")
		}
		kLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4

		if offset+kLen > len(data) {
			return nil, errors.New("wal: invalid batch format")
		}
		key := data[offset : offset+kLen]
		offset += kLen

		if offset+4 > len(data) {
			return nil, errors.New("wal: invalid batch format")
		}
		vLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4

		if offset+vLen > len(data) {
			return nil, errors.New("wal: invalid batch format")
		}
		var value []byte
		if vLen > 0 {
			value = data[offset : offset+vLen]
		}
		offset += vLen

		b.Entries = append(b.Entries, BatchEntry{Op: op, Key: key, Value: value})
	}
	return b, nil
}
