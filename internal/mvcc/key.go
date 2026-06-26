package mvcc

import (
	"bytes"
	"encoding/binary"
)

// EncodeKey appends a bitwise-inverted timestamp to the user key.
// The timestamp is 8 bytes. By bitwise-inverting it, a newer (larger) timestamp
// will result in a smaller uint64 value, which means when compared lexicographically,
// newer versions of the same user key will appear first.
func EncodeKey(userKey []byte, ts uint64) []byte {
	res := make([]byte, len(userKey)+8)
	copy(res, userKey)
	binary.BigEndian.PutUint64(res[len(userKey):], ^ts)
	return res
}

// DecodeKey extracts the user key and timestamp from an MVCC key.
func DecodeKey(mvccKey []byte) (userKey []byte, ts uint64) {
	if len(mvccKey) < 8 {
		return mvccKey, 0
	}
	userKeyLen := len(mvccKey) - 8
	userKey = mvccKey[:userKeyLen]
	ts = ^binary.BigEndian.Uint64(mvccKey[userKeyLen:])
	return userKey, ts
}

// CompareKeys compares two MVCC keys.
// It first compares the user key ascending.
// If user keys are equal, it compares the timestamps descending (which
// naturally happens due to the bitwise inversion in the encoding).
func CompareKeys(a, b []byte) int {
	ukA := UserKey(a)
	ukB := UserKey(b)
	if cmp := bytes.Compare(ukA, ukB); cmp != 0 {
		return cmp
	}
	// If user keys are equal, we can just use bytes.Compare on the full keys
	// since they are the same length (len(userKey) + 8).
	return bytes.Compare(a, b)
}

// UserKey extracts just the user key from an MVCC key.
func UserKey(mvccKey []byte) []byte {
	if len(mvccKey) < 8 {
		return mvccKey
	}
	return mvccKey[:len(mvccKey)-8]
}

// SameUserKey returns true if both MVCC keys share the same user key.
func SameUserKey(a, b []byte) bool {
	return bytes.Equal(UserKey(a), UserKey(b))
}
