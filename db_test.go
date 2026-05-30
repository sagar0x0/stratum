package stratum

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testOptions(dir string) Options {
	opts := DefaultOptions()
	opts.Dir = dir
	return opts
}

func TestDBPutAndGet(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(testOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Put([]byte("key1"), []byte("value1")))

	val, err := db.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}

func TestDBDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(testOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Put([]byte("key1"), []byte("value1")))
	require.NoError(t, db.Delete([]byte("key1")))

	_, err = db.Get([]byte("key1"))
	assert.Equal(t, ErrNotFound, err)
}

func TestDBCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(testOptions(dir))
	require.NoError(t, err)

	require.NoError(t, db.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, db.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, db.Delete([]byte("k1")))

	require.NoError(t, db.Close())

	db2, err := Open(testOptions(dir))
	require.NoError(t, err)
	defer db2.Close()

	_, err = db2.Get([]byte("k1"))
	assert.Equal(t, ErrNotFound, err)

	val, err := db2.Get([]byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestDBConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(testOptions(dir))
	require.NoError(t, err)
	defer db.Close()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				key := []byte(fmt.Sprintf("k%02d-%02d", id, j))
				val := bytes.Repeat([]byte("v"), 50)
				require.NoError(t, db.Put(key, val))
			}
		}(i)
	}

	wg.Wait()

	for i := 0; i < 10; i++ {
		for j := 0; j < 50; j++ {
			key := []byte(fmt.Sprintf("k%02d-%02d", i, j))
			val, err := db.Get(key)
			require.NoError(t, err)
			assert.Equal(t, bytes.Repeat([]byte("v"), 50), val)
		}
	}
}
