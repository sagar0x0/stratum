package wal

import (
	"errors"
	"io"
	"os"
)

type RecoveryResult struct {
	Batches     []*Batch
	CorruptedAt int64
	TruncatedAt int64
	RecordsRead int
	RecordsLost int
}

func Recover(path string) (*RecoveryResult, error) {
	reader, err := NewReader(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &RecoveryResult{TruncatedAt: -1, CorruptedAt: -1}, nil
		}
		return nil, err
	}

	res := &RecoveryResult{
		TruncatedAt: -1,
		CorruptedAt: -1,
	}

	var readErr error

	for {
		record, err := reader.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			if err == ErrCorruptRecord || err == ErrShortRecord {
				res.CorruptedAt = 1 // Indicate corruption was found
				readErr = err
				break
			}
			reader.Close()
			return nil, err
		}

		batch, err := DecodeBatch(record)
		if err != nil {
			res.CorruptedAt = 1
			readErr = err
			break
		}

		res.Batches = append(res.Batches, batch)
		res.RecordsRead++
	}

	reader.Close()

	if res.CorruptedAt != -1 || readErr != nil {
		// Truncate by rewriting the known good batches to a temporary file
		// and renaming it over the original file.
		tmpPath := path + ".tmp"
		w, err := NewWriter(tmpPath)
		if err != nil {
			return nil, err
		}
		for _, b := range res.Batches {
			if err := w.Append(b.Encode()); err != nil {
				w.Close()
				return nil, err
			}
		}
		if err := w.Sync(); err != nil {
			w.Close()
			return nil, err
		}
		w.Close()

		res.RecordsLost = 1 // We lost at least the corrupted one
		if err := os.Rename(tmpPath, path); err != nil {
			return nil, err
		}
		res.TruncatedAt = 1
	}

	return res, nil
}
