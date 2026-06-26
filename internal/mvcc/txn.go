package mvcc

import (
	"sync"

	"github.com/sagar0x0/stratum/internal/hlc"
)

// TxnManager manages MVCC transactions and conflict detection.
type TxnManager struct {
	mu    sync.RWMutex
	clock *hlc.Clock

	// activeTxns tracks the snapshot timestamp of all active transactions.
	activeTxns map[uint64]struct{}

	// latestCommits tracks the commit timestamp of the most recent write to a key.
	latestCommits map[string]uint64
}

// NewTxnManager creates a new transaction manager.
func NewTxnManager(clock *hlc.Clock) *TxnManager {
	return &TxnManager{
		clock:         clock,
		activeTxns:    make(map[uint64]struct{}),
		latestCommits: make(map[string]uint64),
	}
}

// Begin starts a new transaction.
func (tm *TxnManager) Begin() *Txn {
	ts := tm.clock.Now()

	tm.mu.Lock()
	tm.activeTxns[ts] = struct{}{}
	tm.mu.Unlock()

	return &Txn{
		manager:    tm,
		SnapshotTS: ts,
		writeSet:   make(map[string]struct{}),
	}
}

// MinActiveSnapshot returns the oldest active transaction's snapshot timestamp.
func (tm *TxnManager) MinActiveSnapshot() uint64 {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var minTS uint64 = tm.clock.Now()
	for ts := range tm.activeTxns {
		if ts < minTS {
			minTS = ts
		}
	}
	return minTS
}

// Commit attempts to commit a transaction.
// Returns the commit timestamp and true if successful, or 0 and false if conflict.
func (tm *TxnManager) Commit(txn *Txn) (uint64, bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Conflict detection: First-Writer-Wins
	for key := range txn.writeSet {
		if commitTS, ok := tm.latestCommits[key]; ok {
			if commitTS > txn.SnapshotTS {
				// Abort: someone committed a write to this key after our snapshot
				delete(tm.activeTxns, txn.SnapshotTS)
				return 0, false
			}
		}
	}

	// Commit successful
	commitTS := tm.clock.Now()
	for key := range txn.writeSet {
		tm.latestCommits[key] = commitTS
	}
	delete(tm.activeTxns, txn.SnapshotTS)

	return commitTS, true
}

// Abort aborts the transaction.
func (tm *TxnManager) Abort(txn *Txn) {
	tm.mu.Lock()
	delete(tm.activeTxns, txn.SnapshotTS)
	tm.mu.Unlock()
}

// Txn represents an active MVCC transaction.
type Txn struct {
	manager    *TxnManager
	SnapshotTS uint64
	writeSet   map[string]struct{}
}

// AddWrite adds a key to the transaction's write set for conflict detection.
func (t *Txn) AddWrite(userKey []byte) {
	t.writeSet[string(userKey)] = struct{}{}
}

// Commit commits the transaction.
func (t *Txn) Commit() (uint64, bool) {
	return t.manager.Commit(t)
}

// Abort aborts the transaction.
func (t *Txn) Abort() {
	t.manager.Abort(t)
}
