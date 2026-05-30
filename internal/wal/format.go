package wal

const (
	BlockSize  = 32 * 1024 // 32 KB blocks
	HeaderSize = 7         // 4 (CRC) + 2 (length) + 1 (type)
)

type RecordType byte

const (
	RecordFull   RecordType = 1
	RecordFirst  RecordType = 2
	RecordMiddle RecordType = 3
	RecordLast   RecordType = 4
)
