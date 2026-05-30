package stratum

import "time"

type Options struct {
	Dir                 string
	MemTableSize        int64
	WALMaxBatchSize     int
	WALMaxBatchDelay    time.Duration
	SyncWrites          bool
}

func DefaultOptions() Options {
	return Options{
		Dir:                 "",
		MemTableSize:        4 * 1024 * 1024,
		WALMaxBatchSize:     100,
		WALMaxBatchDelay:    10 * time.Millisecond,
		SyncWrites:          false,
	}
}
