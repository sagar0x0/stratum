package lsm

import (
	"path/filepath"
	"testing"

	"github.com/sagar0x0/stratum/internal/memtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlushToL0(t *testing.T) {
	dir := t.TempDir()
	opts := LSMOptions{
		Dir:              dir,
		BlockSize:        4096,
		BloomBitsPerKey:  10,
		BlockCacheSize:   1024 * 1024,
		CompactionRateMB: 50,
		L0StallTrigger:   12,
	}

	tree, err := NewLSMTree(opts)
	require.NoError(t, err)
	defer tree.Close()

	mt := memtable.NewMemTable(1024 * 1024)
	_ = mt.Put([]byte("key1"), []byte("val1"))
	mt.Put([]byte("key2"), []byte("val2"))

	err = tree.Flush(mt)
	require.NoError(t, err)

	val, found, err := tree.Get([]byte("key1"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("val1"), val)

	v := tree.manifest.Current()
	assert.Equal(t, 1, len(v.Levels[0]))
	assert.Equal(t, 0, len(v.Levels[1]))
}

func TestManifestRecovery(t *testing.T) {
	dir := t.TempDir()
	
	// Create and write to manifest
	m, err := OpenManifest(filepath.Join(dir, "MANIFEST"))
	require.NoError(t, err)
	
	edit := &VersionEdit{
		AddedFiles: []FileMetadata{
			{Level: 0, FileNum: 1, FileSize: 100, SmallestKey: []byte("a"), LargestKey: []byte("z"), NumEntries: 10},
		},
		NextFileNum: 2,
	}
	require.NoError(t, m.Apply(edit))
	require.NoError(t, m.Close())

	// Reopen manifest
	m2, err := OpenManifest(filepath.Join(dir, "MANIFEST"))
	require.NoError(t, err)
	defer m2.Close()

	v := m2.Current()
	assert.Equal(t, 1, len(v.Levels[0]))
	assert.Equal(t, uint64(1), v.Levels[0][0].FileNum)
	assert.Equal(t, uint64(2), m2.nextFileNum)
}
