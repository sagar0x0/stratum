package raft

import (
	"encoding/binary"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	"github.com/sagar0x0/stratum/internal/wal"
	pb "github.com/sagar0x0/stratum/proto/raft"
)

type WAL struct {
	w *wal.Writer
}

func OpenWAL(dir string) (*WAL, error) {
	path := filepath.Join(dir, "raft.wal")
	w, err := wal.NewWriter(path)
	if err != nil {
		return nil, err
	}
	return &WAL{w: w}, nil
}

func (w *WAL) SaveState(term uint64, votedFor string) error {
	// Not fully implemented: should write to a separate metadata file
	return nil
}

func (w *WAL) AppendEntries(entries []*pb.LogEntry) error {
	for _, entry := range entries {
		data, err := proto.Marshal(entry)
		if err != nil {
			return err
		}
		
		// Prepend a tag (e.g. 1) to denote LogEntry
		buf := make([]byte, len(data)+1)
		buf[0] = 1
		copy(buf[1:], data)
		
		if err := w.w.Append(buf); err != nil {
			return err
		}
	}
	return w.Sync()
}

func (w *WAL) Sync() error {
	// In the real internal/wal, GroupCommitter syncs.
	// We'll just rely on the OS or writer's Sync if available.
	return nil
}
