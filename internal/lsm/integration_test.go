package lsm

import (
	"fmt"
	"testing"
	"time"

	"github.com/sagar0x0/stratum/internal/memtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOneMillion tests writing 1 million keys and verifying they are retrievable.
// This acts as a comprehensive end-to-end integration test of the LSM tree,
// including MemTable flushes, background compaction, and multi-level reads.
func TestOneMillion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running integration test in short mode")
	}

	dir := t.TempDir()
	opts := LSMOptions{
		Dir:              dir,
		BlockSize:        4096,
		BloomBitsPerKey:  10,
		BlockCacheSize:   8 * 1024 * 1024,
		CompactionRateMB: 50,
		L0StallTrigger:   12,
	}

	tree, err := NewLSMTree(opts)
	require.NoError(t, err)
	tree.StartCompaction()

	const numKeys = 1000000
	const flushThreshold = 50000

	mt := memtable.NewMemTable(8 * 1024 * 1024)

	start := time.Now()
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		val := []byte(fmt.Sprintf("val-%08d", i))
		
		err := mt.Put(key, val)
		require.NoError(t, err, "memtable put failed, maybe capacity is too small")

		if (i+1)%flushThreshold == 0 {
			err := tree.Flush(mt)
			require.NoError(t, err)
			mt = memtable.NewMemTable(8 * 1024 * 1024)
		}
	}

	// Flush remaining
	it := mt.NewIterator()
	it.SeekToFirst()
	if mt.ShouldFlush() || it.Valid() {
		err := tree.Flush(mt)
		require.NoError(t, err)
	}

	t.Logf("Wrote %d keys in %v", numKeys, time.Since(start))

	// Verify all keys
	start = time.Now()
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		expected := []byte(fmt.Sprintf("val-%08d", i))
		
		val, found, err := tree.Get(key)
		if err != nil {
			t.Fatalf("Error reading key %d: %v", i, err)
		}
		if !found {
			t.Fatalf("Key %d not found", i)
		}
		if !assert.Equal(t, expected, val) {
			t.Fatalf("Value mismatch for key %d", i)
		}
	}
	t.Logf("Read %d keys in %v", numKeys, time.Since(start))

	tree.Close()
}
