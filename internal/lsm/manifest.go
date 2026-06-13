package lsm

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
)

const MaxLevels = 7

// FileMetadata stores information about an SSTable file.
type FileMetadata struct {
	Level       int
	FileNum     uint64
	FileSize    uint64
	SmallestKey []byte
	LargestKey  []byte
	NumEntries  uint64
}

// DeletedFile represents an SSTable file that has been deleted.
type DeletedFile struct {
	Level   int
	FileNum uint64
}

// VersionEdit represents an atomic change to the LSM level layout.
type VersionEdit struct {
	AddedFiles   []FileMetadata
	DeletedFiles []DeletedFile
	NextFileNum  uint64 // Must be > 0 if updated
}

// Encode serializes a VersionEdit into a byte slice.
func (v *VersionEdit) Encode() []byte {
	var buf bytes.Buffer

	// Encode NextFileNum (8 bytes, 0 means not updated)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v.NextFileNum)
	buf.Write(tmp[:])

	// Encode DeletedFiles
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(v.DeletedFiles)))
	buf.Write(tmp[:4])
	for _, df := range v.DeletedFiles {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(df.Level))
		buf.Write(tmp[:4])
		binary.LittleEndian.PutUint64(tmp[:8], df.FileNum)
		buf.Write(tmp[:8])
	}

	// Encode AddedFiles
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(v.AddedFiles)))
	buf.Write(tmp[:4])
	for _, af := range v.AddedFiles {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(af.Level))
		buf.Write(tmp[:4])
		binary.LittleEndian.PutUint64(tmp[:8], af.FileNum)
		buf.Write(tmp[:8])
		binary.LittleEndian.PutUint64(tmp[:8], af.FileSize)
		buf.Write(tmp[:8])
		binary.LittleEndian.PutUint64(tmp[:8], af.NumEntries)
		buf.Write(tmp[:8])

		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(af.SmallestKey)))
		buf.Write(tmp[:4])
		buf.Write(af.SmallestKey)

		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(af.LargestKey)))
		buf.Write(tmp[:4])
		buf.Write(af.LargestKey)
	}

	return buf.Bytes()
}

// DecodeVersionEdit deserializes a VersionEdit from a byte slice.
func DecodeVersionEdit(data []byte) (*VersionEdit, error) {
	if len(data) < 8 {
		return nil, os.ErrInvalid
	}

	v := &VersionEdit{}
	offset := 0

	v.NextFileNum = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	if offset+4 > len(data) {
		return nil, os.ErrInvalid
	}
	numDeleted := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	for i := 0; i < numDeleted; i++ {
		if offset+12 > len(data) {
			return nil, os.ErrInvalid
		}
		level := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
		fileNum := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		v.DeletedFiles = append(v.DeletedFiles, DeletedFile{Level: level, FileNum: fileNum})
	}

	if offset+4 > len(data) {
		return nil, os.ErrInvalid
	}
	numAdded := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	for i := 0; i < numAdded; i++ {
		if offset+28 > len(data) {
			return nil, os.ErrInvalid
		}
		level := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
		fileNum := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		fileSize := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		numEntries := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8

		if offset+4 > len(data) {
			return nil, os.ErrInvalid
		}
		smallLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if offset+smallLen > len(data) {
			return nil, os.ErrInvalid
		}
		smallestKey := data[offset : offset+smallLen]
		offset += smallLen

		if offset+4 > len(data) {
			return nil, os.ErrInvalid
		}
		largeLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if offset+largeLen > len(data) {
			return nil, os.ErrInvalid
		}
		largestKey := data[offset : offset+largeLen]
		offset += largeLen

		v.AddedFiles = append(v.AddedFiles, FileMetadata{
			Level:       level,
			FileNum:     fileNum,
			FileSize:    fileSize,
			SmallestKey: smallestKey,
			LargestKey:  largestKey,
			NumEntries:  numEntries,
		})
	}

	return v, nil
}

// Version represents an immutable snapshot of the LSM level layout.
type Version struct {
	Levels [MaxLevels][]FileMetadata
}

// clone creates a deep copy of the Version.
func (v *Version) clone() *Version {
	newV := &Version{}
	for i := 0; i < MaxLevels; i++ {
		newV.Levels[i] = make([]FileMetadata, len(v.Levels[i]))
		copy(newV.Levels[i], v.Levels[i])
	}
	return newV
}

// Manifest persists VersionEdits and reconstructs Versions on recovery.
type Manifest struct {
	file        *os.File
	current     *Version
	nextFileNum uint64
	mu          sync.Mutex
}

// OpenManifest opens a manifest file, replays edits to reconstruct Version, and returns a Manifest.
func OpenManifest(path string) (*Manifest, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	m := &Manifest{
		file:        file,
		current:     &Version{},
		nextFileNum: 1, // Start at 1
	}

	// Replay edits if the file has data
	// For simplicity, we just read the whole file and parse edit by edit.
	// We'll use a length-prefixed format for each edit: [len(4)][data...]
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() > 0 {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		offset := 0
		for offset < len(data) {
			if offset+4 > len(data) {
				break // Corrupt or truncated
			}
			length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
			offset += 4

			if offset+length > len(data) {
				break // Corrupt or truncated
			}
			editData := data[offset : offset+length]
			offset += length

			edit, err := DecodeVersionEdit(editData)
			if err != nil {
				return nil, err
			}

			// Apply edit to current in-memory version
			m.applyEditToMemory(edit)
		}
	}

	return m, nil
}

// applyEditToMemory applies a VersionEdit to the current in-memory Version.
func (m *Manifest) applyEditToMemory(edit *VersionEdit) {
	newV := m.current.clone()

	// Process deletions
	for _, df := range edit.DeletedFiles {
		var newFiles []FileMetadata
		for _, f := range newV.Levels[df.Level] {
			if f.FileNum != df.FileNum {
				newFiles = append(newFiles, f)
			}
		}
		newV.Levels[df.Level] = newFiles
	}

	// Process additions
	for _, af := range edit.AddedFiles {
		newV.Levels[af.Level] = append(newV.Levels[af.Level], af)
	}

	// Sort levels >= 1 since they must be non-overlapping and ordered for binary search
	for i := 1; i < MaxLevels; i++ {
		if len(newV.Levels[i]) > 1 {
			SortBySmallestKey(newV.Levels[i])
		}
	}

	if edit.NextFileNum > m.nextFileNum {
		m.nextFileNum = edit.NextFileNum
	}

	m.current = newV
}

// Apply applies an edit, writes it to disk, and updates the current Version.
func (m *Manifest) Apply(edit *VersionEdit) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data := edit.Encode()
	var prefix [4]byte
	binary.LittleEndian.PutUint32(prefix[:], uint32(len(data)))

	if _, err := m.file.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := m.file.Write(data); err != nil {
		return err
	}
	if err := m.file.Sync(); err != nil {
		return err
	}

	m.applyEditToMemory(edit)
	return nil
}

// Current returns the current Version.
func (m *Manifest) Current() *Version {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

// NextFileNumber returns the next available file number.
func (m *Manifest) NextFileNumber() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	num := m.nextFileNum
	m.nextFileNum++
	return num
}

func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.file.Close()
}
