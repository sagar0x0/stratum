package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, 4096, 10)
	require.NoError(t, err)

	keys := make([][]byte, 1000)
	vals := make([][]byte, 1000)

	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
		vals[i] = []byte(fmt.Sprintf("val-%04d", i))
		require.NoError(t, w.Add(keys[i], vals[i]))
	}

	require.NoError(t, w.Close())

	cache := NewBlockCache(1024 * 1024)
	r, err := OpenReader(path, 1, cache)
	require.NoError(t, err)
	defer r.Close()

	for i := 0; i < 1000; i++ {
		val, found, err := r.Get(keys[i])
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, vals[i], val)
	}

	// Test non-existent key
	_, found, err := r.Get([]byte("missing"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestSSTableBloomSkip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, 4096, 10)
	require.NoError(t, err)

	require.NoError(t, w.Add([]byte("key1"), []byte("val1")))
	require.NoError(t, w.Close())

	r, err := OpenReader(path, 1, nil)
	require.NoError(t, err)
	defer r.Close()

	// Should not read block for missing key
	val, found, err := r.Get([]byte("key2"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestSSTableIterator(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, 1024, 10) // small block size to test block boundaries
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		require.NoError(t, w.Add([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("val-%02d", i))))
	}
	require.NoError(t, w.Close())

	r, err := OpenReader(path, 1, nil)
	require.NoError(t, err)
	defer r.Close()

	it := r.NewIterator()
	i := 0
	for ; it.Valid(); it.Next() {
		assert.Equal(t, []byte(fmt.Sprintf("key-%02d", i)), it.Key())
		assert.Equal(t, []byte(fmt.Sprintf("val-%02d", i)), it.Value())
		i++
	}
	assert.Equal(t, 100, i)
	assert.NoError(t, it.Err())

	// Test Seek
	it.Seek([]byte("key-50"))
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("key-50"), it.Key())

	it.Seek([]byte("key-50x")) // should point to key-51
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("key-51"), it.Key())
}

func TestSSTableTombstones(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	w, err := NewWriter(path, 4096, 10)
	require.NoError(t, err)

	require.NoError(t, w.Add([]byte("key1"), []byte("val1")))
	require.NoError(t, w.Add([]byte("key2"), nil)) // tombstone
	require.NoError(t, w.Add([]byte("key3"), []byte("val3")))
	require.NoError(t, w.Close())

	r, err := OpenReader(path, 1, nil)
	require.NoError(t, err)
	defer r.Close()

	val, found, err := r.Get([]byte("key2"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Nil(t, val)

	it := r.NewIterator()
	it.SeekToFirst()
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("key1"), it.Key())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("key2"), it.Key())
	assert.Nil(t, it.Value())
	assert.True(t, it.IsTombstone())

	it.Next()
	assert.True(t, it.Valid())
	assert.Equal(t, []byte("key3"), it.Key())
}

func TestBlockCache(t *testing.T) {
	cache := NewBlockCache(1024)
	cache.Put(1, 100, []byte("data1"))
	cache.Put(1, 200, []byte("data2"))
	cache.Put(2, 100, []byte("data3"))

	val, found := cache.Get(1, 100)
	assert.True(t, found)
	assert.Equal(t, []byte("data1"), val)

	cache.RemoveFile(1)
	_, found = cache.Get(1, 100)
	assert.False(t, found)

	val, found = cache.Get(2, 100)
	assert.True(t, found)
	assert.Equal(t, []byte("data3"), val)
}
