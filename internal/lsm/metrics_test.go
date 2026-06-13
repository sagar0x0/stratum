package lsm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompactionStats(t *testing.T) {
	stats := &CompactionStats{}
	stats.RecordCompaction(1024, 512, 100*time.Millisecond)
	stats.RecordStall(50 * time.Millisecond)
	stats.IncrPending()

	snap := stats.Snapshot()
	assert.Equal(t, int64(1024), snap.BytesCompacted)
	assert.Equal(t, int64(512), snap.BytesWritten)
	assert.Equal(t, int64(1), snap.CompactionCount)
	assert.Equal(t, int64(100), snap.CompactionTimeMs)
	assert.Equal(t, int64(50), snap.StallTimeMs)
	assert.Equal(t, int32(1), snap.PendingCompactions)

	stats.DecrPending()
	snap = stats.Snapshot()
	assert.Equal(t, int32(0), snap.PendingCompactions)
}
