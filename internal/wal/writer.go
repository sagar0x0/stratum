package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type Writer struct {
	file      *os.File
	blockBuf  []byte
	blockUsed int
	mu        sync.Mutex
	closed    bool
}

func NewWriter(path string) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Writer{
		file:     file,
		blockBuf: make([]byte, BlockSize),
	}, nil
}

func (w *Writer) Append(record []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	left := len(record)
	ptr := 0
	first := true

	for {
		rem := BlockSize - w.blockUsed
		if rem < HeaderSize {
			if rem > 0 {
				for i := 0; i < rem; i++ {
					w.blockBuf[w.blockUsed+i] = 0
				}
				w.blockUsed += rem
			}
			if err := w.flushBlock(); err != nil {
				return err
			}
			rem = BlockSize
		}

		avail := rem - HeaderSize
		frag := left
		if frag > avail {
			frag = avail
		}

		var typ RecordType
		if first && frag == left {
			typ = RecordFull
		} else if first {
			typ = RecordFirst
		} else if frag == left {
			typ = RecordLast
		} else {
			typ = RecordMiddle
		}

		if err := w.emitPhysicalRecord(typ, record[ptr:ptr+frag]); err != nil {
			return err
		}

		ptr += frag
		left -= frag
		first = false

		if left <= 0 {
			break
		}
	}
	return nil
}

func (w *Writer) emitPhysicalRecord(typ RecordType, payload []byte) error {
	length := len(payload)

	binary.LittleEndian.PutUint16(w.blockBuf[w.blockUsed+4:], uint16(length))
	w.blockBuf[w.blockUsed+6] = byte(typ)

	copy(w.blockBuf[w.blockUsed+7:], payload)

	crc := crc32.Update(0, castagnoliTable, []byte{byte(typ)})
	crc = crc32.Update(crc, castagnoliTable, payload)
	binary.LittleEndian.PutUint32(w.blockBuf[w.blockUsed:], crc)

	w.blockUsed += HeaderSize + length
	return nil
}

func (w *Writer) flushBlock() error {
	if w.blockUsed == 0 {
		return nil
	}
	_, err := w.file.Write(w.blockBuf[:w.blockUsed])
	if err != nil {
		return err
	}
	w.blockUsed = 0
	return nil
}

func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	if err := w.flushBlock(); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.flushBlock(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}
