package lsm

import (
	"sync"
	"time"
)

// RateLimiter controls the I/O rate for background compactions.
type RateLimiter struct {
	bytesPerSec int64
	available   int64
	mu          sync.Mutex
	cond        *sync.Cond
	ticker      *time.Ticker
	stopCh      chan struct{}
}

// NewRateLimiter creates a new RateLimiter.
func NewRateLimiter(bytesPerSec int64) *RateLimiter {
	if bytesPerSec <= 0 {
		return nil // No limit
	}
	r := &RateLimiter{
		bytesPerSec: bytesPerSec,
		available:   bytesPerSec,
		stopCh:      make(chan struct{}),
	}
	r.cond = sync.NewCond(&r.mu)
	
	// Refill 10 times a second
	r.ticker = time.NewTicker(100 * time.Millisecond)
	go r.refillLoop()
	
	return r
}

func (r *RateLimiter) refillLoop() {
	refillAmount := r.bytesPerSec / 10
	if refillAmount == 0 {
		refillAmount = 1
	}

	for {
		select {
		case <-r.ticker.C:
			r.mu.Lock()
			r.available += refillAmount
			if r.available > r.bytesPerSec {
				r.available = r.bytesPerSec // cap at burst size
			}
			r.cond.Broadcast()
			r.mu.Unlock()
		case <-r.stopCh:
			return
		}
	}
}

// Request blocks until the specified number of bytes can be processed.
func (r *RateLimiter) Request(bytes int64) {
	if r == nil {
		return
	}
	
	r.mu.Lock()
	defer r.mu.Unlock()

	for bytes > 0 {
		if r.available >= bytes {
			r.available -= bytes
			return
		}

		if r.available > 0 {
			bytes -= r.available
			r.available = 0
		}
		
		r.cond.Wait()
	}
}

// Stop stops the rate limiter's background goroutine.
func (r *RateLimiter) Stop() {
	if r == nil {
		return
	}
	r.ticker.Stop()
	close(r.stopCh)
}
