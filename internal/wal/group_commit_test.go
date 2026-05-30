package wal

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupCommitSingleWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	gc := NewGroupCommitter(w)
	gc.Start()

	for i := 0; i < 100; i++ {
		b := &Batch{}
		b.Put([]byte("key"), []byte("val"))
		require.NoError(t, gc.Submit(b.Encode()))
	}

	gc.Stop()

	res, err := Recover(path)
	require.NoError(t, err)
	assert.Equal(t, 100, res.RecordsRead)
}

func TestGroupCommitConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	gc := NewGroupCommitter(w)
	gc.Start()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b := &Batch{}
				b.Put([]byte("key"), []byte("val"))
				require.NoError(t, gc.Submit(b.Encode()))
			}
		}()
	}

	wg.Wait()
	gc.Stop()

	res, err := Recover(path)
	require.NoError(t, err)
	assert.Equal(t, 1000, res.RecordsRead)
}

func TestGroupCommitBatchAmortization(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	gc := NewGroupCommitter(w, WithMaxBatchSize(100), WithMaxBatchDelay(10*time.Millisecond))
	gc.Start()

	var count int32
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := &Batch{}
			b.Put([]byte("key"), []byte("val"))
			require.NoError(t, gc.Submit(b.Encode()))
			atomic.AddInt32(&count, 1)
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	assert.Equal(t, int32(100), count)
	assert.Less(t, duration, 200*time.Millisecond)

	gc.Stop()
}
