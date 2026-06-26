package hlc

import (
	"sync"
	"time"
)

// Clock implements a Hybrid Logical Clock.
type Clock struct {
	mu      sync.Mutex
	wall    int64
	logical uint32
}

// NewClock creates a new Hybrid Logical Clock.
func NewClock() *Clock {
	return &Clock{}
}

// Now returns the current HLC timestamp.
// The top 48 bits are physical wall time in milliseconds.
// The bottom 16 bits are the logical counter.
func (c *Clock) Now() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UnixMilli()
	if now > c.wall {
		c.wall = now
		c.logical = 0
	} else {
		c.logical++
	}

	return (uint64(c.wall) << 16) | uint64(c.logical)
}

// Update advances the local clock based on a timestamp received from a remote node.
func (c *Clock) Update(remote uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UnixMilli()
	remoteWall := int64(remote >> 16)
	remoteLogical := uint32(remote & 0xFFFF)

	if now > c.wall && now > remoteWall {
		c.wall = now
		c.logical = 0
	} else if c.wall >= now && c.wall >= remoteWall {
		if c.wall == remoteWall {
			if remoteLogical > c.logical {
				c.logical = remoteLogical
			}
		}
		c.logical++
	} else if remoteWall >= now && remoteWall >= c.wall {
		c.wall = remoteWall
		c.logical = remoteLogical + 1
	}
}
