package bloom

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomBasicMembership(t *testing.T) {
	filter := NewFilter(1000, 10)

	// Add 1000 keys
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		filter.Add(keys[i])
	}

	// Verify all return true
	for i := 0; i < 1000; i++ {
		assert.True(t, filter.MayContain(keys[i]), "Filter must contain key %d", i)
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	numKeys := 100000
	bitsPerKey := 10

	filter := NewFilter(numKeys, bitsPerKey)

	for i := 0; i < numKeys; i++ {
		filter.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	falsePositives := 0
	for i := 0; i < numKeys; i++ {
		if filter.MayContain([]byte(fmt.Sprintf("nonexistent-%d", i))) {
			falsePositives++
		}
	}

	fpr := float64(falsePositives) / float64(numKeys)
	t.Logf("False positive rate: %.4f%%", fpr*100)

	// With 10 bits per key, expected FPR is ~1%
	assert.Less(t, fpr, 0.015, "False positive rate should be less than 1.5%")
}

func TestBloomEncodeDecode(t *testing.T) {
	filter := NewFilter(100, 10)
	for i := 0; i < 100; i++ {
		filter.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	encoded := filter.Encode()
	decoded := DecodeFilter(encoded)

	assert.Equal(t, filter.nHash, decoded.nHash)
	assert.Equal(t, filter.numBits, decoded.numBits)
	assert.Equal(t, filter.bits, decoded.bits)

	for i := 0; i < 100; i++ {
		assert.True(t, decoded.MayContain([]byte(fmt.Sprintf("key-%d", i))))
	}

	assert.False(t, decoded.MayContain([]byte("missing-key")))
}

func TestBloomEmpty(t *testing.T) {
	filter := NewFilter(0, 0)
	assert.False(t, filter.MayContain([]byte("any")))

	decoded := DecodeFilter(nil)
	assert.False(t, decoded.MayContain([]byte("any")))
}

func TestBloomSmall(t *testing.T) {
	filter := NewFilter(1, 10)
	filter.Add([]byte("hello"))
	assert.True(t, filter.MayContain([]byte("hello")))
	assert.False(t, filter.MayContain([]byte("world")))
}
