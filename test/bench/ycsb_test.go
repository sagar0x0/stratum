package bench

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sagar0x0/stratum"
)

func runYCSBBenchmark(b *testing.B, workload Workload, numKeys, ops, keySize, valSize int) {
	dir := b.TempDir()
	opts := stratum.DefaultOptions()
	opts.Dir = dir
	opts.MemTableSize = 16 * 1024 * 1024
	opts.BlockCacheSize = 64 * 1024 * 1024

	db, err := stratum.Open(opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	y := NewYCSB(db, numKeys, keySize, valSize)

	// Load phase
	b.Logf("Loading %d keys...", numKeys)
	if err := y.Load(); err != nil {
		b.Fatalf("Load failed: %v", err)
	}

	b.ResetTimer()
	b.Logf("Running workload with %d ops...", ops)

	start := time.Now()
	for i := 0; i < b.N; i++ {
		if err := y.Run(workload, ops); err != nil {
			b.Fatalf("Run failed: %v", err)
		}
	}
	duration := time.Since(start)

	b.StopTimer()

	// Print custom metrics
	if b.N > 0 {
		opsPerSec := float64(ops*b.N) / duration.Seconds()
		b.ReportMetric(opsPerSec, "ops/sec")
		
		fmt.Printf("Workload Throughput: %.2f ops/sec\n", opsPerSec)
	}
}

func BenchmarkYCSB_WorkloadA(b *testing.B) {
	runYCSBBenchmark(b, WorkloadA, 10000, 10000, 16, 256)
}

func BenchmarkYCSB_WorkloadB(b *testing.B) {
	runYCSBBenchmark(b, WorkloadB, 10000, 10000, 16, 256)
}

func BenchmarkYCSB_WorkloadC(b *testing.B) {
	runYCSBBenchmark(b, WorkloadC, 10000, 10000, 16, 256)
}

func BenchmarkYCSB_WorkloadF(b *testing.B) {
	runYCSBBenchmark(b, WorkloadF, 10000, 10000, 16, 256)
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
