package stratum

import "time"

type Options struct {
	Dir                 string
	MemTableSize        int64
	WALMaxBatchSize     int
	WALMaxBatchDelay    time.Duration
	SyncWrites          bool
	SSTableBlockSize    int
	BloomBitsPerKey     int
	BlockCacheSize      int64
	CompactionRateMB    int
	L0StallTrigger      int
}

func DefaultOptions() Options {
	return Options{
		Dir:                 "",
		MemTableSize:        4 * 1024 * 1024,
		WALMaxBatchSize:     100,
		WALMaxBatchDelay:    10 * time.Millisecond,
		SyncWrites:          false,
		SSTableBlockSize:    4096,
		BloomBitsPerKey:     10,
		BlockCacheSize:      8 * 1024 * 1024,
		CompactionRateMB:    50,
		L0StallTrigger:      12,
	}
}
