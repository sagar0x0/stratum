package memtable

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/sagar0x0/stratum/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupManager(t *testing.T, flushFn FlushFunc) (*Manager, *wal.GroupCommitter, string) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := wal.NewWriter(path)
	require.NoError(t, err)

	gc := wal.NewGroupCommitter(w)
	gc.Start()

	mgr := NewManager(gc, 1024, flushFn)
	mgr.Start()

	return mgr, gc, path
}

func TestManagerPutAndGet(t *testing.T) {
	mgr, gc, _ := setupManager(t, func(mt *MemTable) error { return nil })
	defer gc.Stop()
	defer mgr.Stop()

	require.NoError(t, mgr.Put([]byte("key"), []byte("value")))
	val, found := mgr.Get([]byte("key"))
	assert.True(t, found)
	assert.Equal(t, []byte("value"), val)
}

func TestManagerWritesDurableViaWAL(t *testing.T) {
	mgr, gc, path := setupManager(t, func(mt *MemTable) error { return nil })

	require.NoError(t, mgr.Put([]byte("key"), []byte("value")))

	mgr.Stop()
	gc.Stop()

	res, err := wal.Recover(path)
	require.NoError(t, err)
	assert.Equal(t, 1, res.RecordsRead)
}

func TestManagerFreezeAndSwap(t *testing.T) {
	flushed := make(chan struct{}, 1)
	mgr, gc, _ := setupManager(t, func(mt *MemTable) error {
		select {
		case flushed <- struct{}{}:
		default:
		}
		return nil
	})
	defer gc.Stop()
	defer mgr.Stop()

	require.NoError(t, mgr.Put([]byte("large_key"), bytes.Repeat([]byte("v"), 2000)))

	select {
	case <-flushed:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Flush callback not called")
	}
}

func TestManagerReadFallsThrough(t *testing.T) {
	flushBlock := make(chan struct{})
	mgr, gc, _ := setupManager(t, func(mt *MemTable) error {
		<-flushBlock
		return nil
	})
	defer gc.Stop()
	defer mgr.Stop()

	require.NoError(t, mgr.Put([]byte("k1"), []byte("v1")))

	require.NoError(t, mgr.Put([]byte("filler"), bytes.Repeat([]byte("v"), 2000)))

	val, found := mgr.Get([]byte("k1"))
	assert.True(t, found)
	assert.Equal(t, []byte("v1"), val)

	close(flushBlock)
}

func TestManagerBackPressure(t *testing.T) {
	flushBlock := make(chan struct{})
	flushed := make(chan struct{}, 1)
	mgr, gc, _ := setupManager(t, func(mt *MemTable) error {
		<-flushBlock
		select {
		case flushed <- struct{}{}:
		default:
		}
		return nil
	})
	defer gc.Stop()
	defer mgr.Stop()

	require.NoError(t, mgr.Put([]byte("filler1"), bytes.Repeat([]byte("v"), 2000)))

	done := make(chan struct{})
	go func() {
		require.NoError(t, mgr.Put([]byte("filler2"), bytes.Repeat([]byte("v"), 2000)))
		require.NoError(t, mgr.Put([]byte("filler3"), bytes.Repeat([]byte("v"), 2000)))
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Put should have blocked due to backpressure")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}

	close(flushBlock)

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Put didn't unblock after flush completed")
	}
}

func TestManagerCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := wal.NewWriter(path)
	require.NoError(t, err)

	gc := wal.NewGroupCommitter(w)
	gc.Start()

	mgr := NewManager(gc, 1024, func(mt *MemTable) error { return nil })
	mgr.Start()

	require.NoError(t, mgr.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, mgr.Delete([]byte("k1")))
	require.NoError(t, mgr.Put([]byte("k2"), []byte("v2")))

	mgr.Stop()
	gc.Stop()

	res, err := wal.Recover(path)
	require.NoError(t, err)

	newMt := NewMemTable(1024 * 1024)
	for _, b := range res.Batches {
		require.NoError(t, newMt.ApplyBatch(b))
	}

	_, found := newMt.Get([]byte("k1"))
	assert.True(t, found)
	val, _ := newMt.Get([]byte("k1"))
	assert.Nil(t, val)

	val2, found2 := newMt.Get([]byte("k2"))
	assert.True(t, found2)
	assert.Equal(t, []byte("v2"), val2)
}
