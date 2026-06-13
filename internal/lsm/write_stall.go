package lsm

import "time"

// write_stall logic is primarily state tracking inside the LSMTree.
// We put the methods here to keep the files organized.

// CheckWriteStall evaluates whether writes should be stalled due to L0 accumulation.
// Called after each flush to L0.
func (l *LSMTree) CheckWriteStall() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l0Count := len(l.manifest.Current().Levels[0])
	if l0Count >= l.opts.L0StallTrigger {
		l.stalled = true
	} else {
		l.stalled = false
		l.stallCond.Broadcast() // Wake up any waiting writers
	}
}

// WaitForStall blocks if write stall is active.
// Called before accepting new flushes from the MemTable manager.
func (l *LSMTree) WaitForStall() {
	l.mu.Lock()
	defer l.mu.Unlock()

	start := time.Now()
	stalled := false
	for l.stalled {
		stalled = true
		l.stallCond.Wait()
	}
	if stalled {
		l.stats.RecordStall(time.Since(start))
	}
}

// ReleaseStall is called when compaction reduces L0 below threshold.
func (l *LSMTree) ReleaseStall() {
	l.CheckWriteStall()
}
