package lsm

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/sagar0x0/stratum/internal/memtable"
)

func benchmarkLSMTree(b *testing.B, random bool, readPercent int) {
	dir := b.TempDir()
	opts := LSMOptions{
		Dir:              dir,
		BlockSize:        4096,
		BloomBitsPerKey:  10,
		BlockCacheSize:   16 * 1024 * 1024,
		CompactionRateMB: 100,
		L0StallTrigger:   12,
	}

	tree, err := NewLSMTree(opts)
	if err != nil {
		b.Fatalf("Failed to create LSM tree: %v", err)
	}
	defer tree.Close()

	mt := memtable.NewMemTable(4 * 1024 * 1024)
	val := make([]byte, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isRead := rand.Intn(100) < readPercent
		
		var keyIdx int
		if random && i > 0 {
			keyIdx = rand.Intn(i)
		} else {
			keyIdx = i
		}
		key := []byte(fmt.Sprintf("key-%010d", keyIdx))

		if isRead {
			_, _, _ = tree.Get(key)
		} else {
			mt.Put(key, val)
			if mt.ShouldFlush() {
				b.StopTimer()
				_ = tree.Flush(mt)
				mt = memtable.NewMemTable(4 * 1024 * 1024)
				b.StartTimer()
			}
		}
	}
}

func BenchmarkSequentialWrite(b *testing.B) {
	benchmarkLSMTree(b, false, 0)
}

func BenchmarkRandomWrite(b *testing.B) {
	benchmarkLSMTree(b, true, 0)
}

func BenchmarkMixedWorkload(b *testing.B) {
	benchmarkLSMTree(b, true, 50)
}
