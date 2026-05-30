package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

type Reader struct {
	file      *os.File
	blockBuf  []byte
	blockUsed int
	blockOff  int
	eof       bool
}

func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		file:     file,
		blockBuf: make([]byte, BlockSize),
	}, nil
}

func (r *Reader) ReadRecord() ([]byte, error) {
	record := []byte{}
	inFragmentedRecord := false

	for {
		if r.blockOff+HeaderSize > r.blockUsed {
			if !r.eof {
				if err := r.readBlock(); err != nil {
					if err == io.EOF {
						if inFragmentedRecord {
							return nil, ErrShortRecord
						}
						return nil, io.EOF
					}
					return nil, err
				}
				continue
			} else {
				if inFragmentedRecord {
					return nil, ErrShortRecord
				}
				return nil, io.EOF
			}
		}

		crcExpected := binary.LittleEndian.Uint32(r.blockBuf[r.blockOff:])
		length := int(binary.LittleEndian.Uint16(r.blockBuf[r.blockOff+4:]))
		typ := RecordType(r.blockBuf[r.blockOff+6])

		if length == 0 && typ == 0 {
			// Zero padding reached, advance to next block
			r.blockOff = r.blockUsed
			continue
		}

		if r.blockOff+HeaderSize+length > r.blockUsed {
			return nil, ErrShortRecord
		}

		crcActual := crc32.Update(0, castagnoliTable, []byte{byte(typ)})
		crcActual = crc32.Update(crcActual, castagnoliTable, r.blockBuf[r.blockOff+HeaderSize:r.blockOff+HeaderSize+length])

		if crcExpected != crcActual {
			return nil, ErrCorruptRecord
		}

		payload := r.blockBuf[r.blockOff+HeaderSize : r.blockOff+HeaderSize+length]
		r.blockOff += HeaderSize + length

		if !inFragmentedRecord {
			if typ == RecordFirst || typ == RecordFull {
				record = append(record, payload...)
				inFragmentedRecord = (typ == RecordFirst)
				if typ == RecordFull {
					return record, nil
				}
			} else {
				return nil, ErrCorruptRecord
			}
		} else {
			if typ == RecordMiddle || typ == RecordLast {
				record = append(record, payload...)
				if typ == RecordLast {
					return record, nil
				}
			} else {
				return nil, ErrCorruptRecord
			}
		}
	}
}

func (r *Reader) readBlock() error {
	n, err := io.ReadFull(r.file, r.blockBuf)
	if err != nil {
		if err == io.EOF {
			r.eof = true
			return io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			r.eof = true
			r.blockUsed = n
			r.blockOff = 0
			return nil
		}
		return err
	}
	r.blockUsed = n
	r.blockOff = 0
	return nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}
