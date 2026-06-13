package bloom

import (
	"math"

	"github.com/spaolacci/murmur3"
)

// Filter is a Bloom filter implementation.
type Filter struct {
	bits    []byte
	nHash   uint32
	numBits uint32
}

// NewFilter creates a new Bloom filter for the expected number of keys
// and the desired bits per key.
func NewFilter(numKeys int, bitsPerKey int) *Filter {
	if numKeys < 0 {
		numKeys = 0
	}
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}

	numBits := uint32(numKeys * bitsPerKey)
	if numBits < 64 {
		numBits = 64
	}

	// Round up to nearest byte
	bytes := (numBits + 7) / 8
	numBits = bytes * 8 // adjust to exact multiple of 8

	// nHash = bitsPerKey * ln(2)
	nHash := uint32(float64(bitsPerKey) * math.Ln2)
	if nHash < 1 {
		nHash = 1
	} else if nHash > 30 {
		nHash = 30
	}

	return &Filter{
		bits:    make([]byte, bytes),
		nHash:   nHash,
		numBits: numBits,
	}
}

// Add inserts a key into the Bloom filter.
func (f *Filter) Add(key []byte) {
	if len(f.bits) == 0 {
		return
	}

	h1, h2 := murmur3.Sum128(key)
	
	// Double hashing: h(i) = (h1 + i * h2) % numBits
	hash1 := uint32(h1)
	hash2 := uint32(h2)

	for i := uint32(0); i < f.nHash; i++ {
		bitPos := (hash1 + i*hash2) % f.numBits
		f.bits[bitPos/8] |= 1 << (bitPos % 8)
	}
}

// MayContain returns true if the key may be in the filter.
// It returns false if the key is definitely not in the filter.
func (f *Filter) MayContain(key []byte) bool {
	if len(f.bits) == 0 {
		return false
	}

	h1, h2 := murmur3.Sum128(key)
	hash1 := uint32(h1)
	hash2 := uint32(h2)

	for i := uint32(0); i < f.nHash; i++ {
		bitPos := (hash1 + i*hash2) % f.numBits
		if f.bits[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the Bloom filter for storage.
// Format: [1 byte nHash] [var bytes bits]
func (f *Filter) Encode() []byte {
	buf := make([]byte, 1+len(f.bits))
	buf[0] = byte(f.nHash)
	copy(buf[1:], f.bits)
	return buf
}

// DecodeFilter deserializes a Bloom filter from data.
func DecodeFilter(data []byte) *Filter {
	if len(data) < 1 {
		return &Filter{bits: nil, nHash: 0, numBits: 0}
	}

	nHash := uint32(data[0])
	bits := data[1:]
	numBits := uint32(len(bits) * 8)

	return &Filter{
		bits:    bits,
		nHash:   nHash,
		numBits: numBits,
	}
}
