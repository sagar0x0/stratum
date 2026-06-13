package lsm

import (
	"sync"
	"sync/atomic"
	"time"
)

// CompactionStats tracks metrics related to compaction and write stalls.
type CompactionStats struct {
	mu                sync.Mutex
	BytesCompacted    int64
	BytesWritten      int64
	CompactionCount   int64
	CompactionTimeMs  int64
	StallTimeMs       int64
	pendingCompactions int32
}

// CompactionStatsSnapshot is a thread-safe copy of CompactionStats.
type CompactionStatsSnapshot struct {
	BytesCompacted    int64
	BytesWritten      int64
	CompactionCount   int64
	CompactionTimeMs  int64
	StallTimeMs       int64
	PendingCompactions int32
}

// RecordCompaction records a completed compaction.
func (s *CompactionStats) RecordCompaction(bytesIn, bytesOut int64, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.BytesCompacted += bytesIn
	s.BytesWritten += bytesOut
	s.CompactionCount++
	s.CompactionTimeMs += duration.Milliseconds()
}

// RecordStall records time spent in write stall.
func (s *CompactionStats) RecordStall(duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.StallTimeMs += duration.Milliseconds()
}

// IncrPending increments the pending compaction count.
func (s *CompactionStats) IncrPending() {
	atomic.AddInt32(&s.pendingCompactions, 1)
}

// DecrPending decrements the pending compaction count.
func (s *CompactionStats) DecrPending() {
	atomic.AddInt32(&s.pendingCompactions, -1)
}

// Snapshot returns a point-in-time copy of the stats.
func (s *CompactionStats) Snapshot() CompactionStatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return CompactionStatsSnapshot{
		BytesCompacted:    s.BytesCompacted,
		BytesWritten:      s.BytesWritten,
		CompactionCount:   s.CompactionCount,
		CompactionTimeMs:  s.CompactionTimeMs,
		StallTimeMs:       s.StallTimeMs,
		PendingCompactions: atomic.LoadInt32(&s.pendingCompactions),
	}
}
