package wal

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAndReadSingleRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	record := []byte("hello world")
	require.NoError(t, w.Append(record))
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	r, err := NewReader(path)
	require.NoError(t, err)

	out, err := r.ReadRecord()
	require.NoError(t, err)
	assert.Equal(t, record, out)

	_, err = r.ReadRecord()
	assert.Equal(t, io.EOF, err)
	require.NoError(t, r.Close())
}

func TestWriteAndReadMultipleRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	var records [][]byte
	for i := 0; i < 100; i++ {
		record := bytes.Repeat([]byte{byte(i)}, i*10)
		records = append(records, record)
		require.NoError(t, w.Append(record))
	}
	require.NoError(t, w.Close())

	r, err := NewReader(path)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		out, err := r.ReadRecord()
		require.NoError(t, err)
		assert.Equal(t, records[i], out)
	}

	_, err = r.ReadRecord()
	assert.Equal(t, io.EOF, err)
	require.NoError(t, r.Close())
}

func TestRecordSpansBlockBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	paddingSize := BlockSize - HeaderSize - 100
	padding := bytes.Repeat([]byte("A"), paddingSize)
	require.NoError(t, w.Append(padding))

	spanRecord := bytes.Repeat([]byte("B"), 200)
	require.NoError(t, w.Append(spanRecord))
	require.NoError(t, w.Close())

	r, err := NewReader(path)
	require.NoError(t, err)

	out1, err := r.ReadRecord()
	require.NoError(t, err)
	assert.Equal(t, padding, out1)

	out2, err := r.ReadRecord()
	require.NoError(t, err)
	assert.Equal(t, spanRecord, out2)

	require.NoError(t, r.Close())
}

func TestLargeRecordSpansMultipleBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)

	largeRecord := bytes.Repeat([]byte("L"), 100000)
	require.NoError(t, w.Append(largeRecord))
	require.NoError(t, w.Close())

	r, err := NewReader(path)
	require.NoError(t, err)

	out, err := r.ReadRecord()
	require.NoError(t, err)
	assert.Equal(t, largeRecord, out)
	require.NoError(t, r.Close())
}

func TestCRC32Integrity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)
	require.NoError(t, w.Append([]byte("corrupt me")))
	require.NoError(t, w.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[10] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0644))

	r, err := NewReader(path)
	require.NoError(t, err)

	_, err = r.ReadRecord()
	assert.Equal(t, ErrCorruptRecord, err)
	require.NoError(t, r.Close())
}

func TestEmptyRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWriter(path)
	require.NoError(t, err)
	require.NoError(t, w.Append([]byte{}))
	require.NoError(t, w.Close())

	r, err := NewReader(path)
	require.NoError(t, err)

	out, err := r.ReadRecord()
	require.NoError(t, err)
	assert.Equal(t, []byte{}, out)
	require.NoError(t, r.Close())
}
