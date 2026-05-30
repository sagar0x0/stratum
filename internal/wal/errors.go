package wal

import "errors"

var (
	ErrCorruptRecord = errors.New("wal: corrupt record (CRC mismatch)")
	ErrShortRecord   = errors.New("wal: short record (truncated write)")
	ErrClosed        = errors.New("wal: writer is closed")
)
