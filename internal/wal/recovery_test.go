package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverCleanWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		b := &Batch{}
		b.Put([]byte("key"), []byte("val"))
		require.NoError(t, w.Append(b.Encode()))
	}
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	res, err := Recover(path)
	require.NoError(t, err)

	assert.Equal(t, 10, res.RecordsRead)
	assert.Equal(t, 10, len(res.Batches))
	assert.Equal(t, int64(-1), res.CorruptedAt)
	assert.Equal(t, int64(-1), res.TruncatedAt)
	assert.Equal(t, 0, res.RecordsLost)
}

func TestRecoverCorruptedLastRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		b := &Batch{}
		b.Put([]byte("key"), []byte("val"))
		require.NoError(t, w.Append(b.Encode()))
	}
	require.NoError(t, w.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(data)-5] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0644))

	res, err := Recover(path)
	require.NoError(t, err)

	assert.Equal(t, 9, res.RecordsRead)
	assert.Equal(t, 9, len(res.Batches))
	assert.NotEqual(t, int64(-1), res.CorruptedAt)
	assert.NotEqual(t, int64(-1), res.TruncatedAt)
	assert.Equal(t, 1, res.RecordsLost)

	res2, err := Recover(path)
	require.NoError(t, err)
	assert.Equal(t, 9, res2.RecordsRead)
	assert.Equal(t, int64(-1), res2.CorruptedAt)
}

func TestRecoverTruncatedWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		b := &Batch{}
		b.Put([]byte("key"), []byte("val"))
		require.NoError(t, w.Append(b.Encode()))
	}
	require.NoError(t, w.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data[:len(data)-10], 0644))

	res, err := Recover(path)
	require.NoError(t, err)

	assert.Equal(t, 9, res.RecordsRead)
	assert.Equal(t, 9, len(res.Batches))
	assert.NotEqual(t, int64(-1), res.CorruptedAt)
	assert.NotEqual(t, int64(-1), res.TruncatedAt)
	assert.Equal(t, 1, res.RecordsLost)
}

func TestRecoverEmptyWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	res, err := Recover(path)
	require.NoError(t, err)
	assert.Equal(t, 0, res.RecordsRead)
	assert.Equal(t, int64(-1), res.CorruptedAt)
}

func TestRecoverCorruptedMiddleRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		b := &Batch{}
		b.Put(bytes.Repeat([]byte("k"), 100), bytes.Repeat([]byte("v"), 100))
		require.NoError(t, w.Append(b.Encode()))
	}
	require.NoError(t, w.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(data)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0644))

	res, err := Recover(path)
	require.NoError(t, err)

	assert.Equal(t, 2, res.RecordsRead)
	assert.Equal(t, 2, len(res.Batches))
	assert.NotEqual(t, int64(-1), res.CorruptedAt)
}
