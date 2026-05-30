package memtable

import (
	"log"
	"sync"
	"time"

	"github.com/sagar0x0/stratum/internal/wal"
)

type FlushFunc func(mt *MemTable) error

type Manager struct {
	mu        sync.RWMutex
	cond      *sync.Cond
	active    *MemTable
	immutable *MemTable

	wal     *wal.GroupCommitter
	maxSize int64
	flushFn FlushFunc

	flushCh chan struct{}
	stopCh  chan struct{}
	flushWg sync.WaitGroup
}

func NewManager(walCommitter *wal.GroupCommitter, maxSize int64, flushFn FlushFunc) *Manager {
	m := &Manager{
		active:  NewMemTable(maxSize),
		wal:     walCommitter,
		maxSize: maxSize,
		flushFn: flushFn,
		flushCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *Manager) Start() {
	m.flushWg.Add(1)
	go m.flushLoop()
}

func (m *Manager) Stop() {
	close(m.stopCh)
	m.flushWg.Wait()
}

func (m *Manager) Put(key, value []byte) error {
	batch := &wal.Batch{}
	batch.Put(key, value)
	if err := m.wal.Submit(batch.Encode()); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for {
		err := m.active.Put(key, value)
		if err == nil {
			if m.active.ShouldFlush() && m.immutable == nil {
				m.active.Freeze()
				m.immutable = m.active
				m.active = NewMemTable(m.maxSize)
				select {
				case m.flushCh <- struct{}{}:
				default:
				}
			}
			return nil
		}

		if err == ErrMemTableFull {
			if m.immutable != nil {
				m.cond.Wait()
				continue
			}
			m.active.Freeze()
			m.immutable = m.active
			m.active = NewMemTable(m.maxSize)
			select {
			case m.flushCh <- struct{}{}:
			default:
			}
			continue
		}

		return err
	}
}

func (m *Manager) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	active := m.active
	immutable := m.immutable
	m.mu.RUnlock()

	if val, found := active.Get(key); found {
		return val, true
	}
	if immutable != nil {
		return immutable.Get(key)
	}
	return nil, false
}

func (m *Manager) Delete(key []byte) error {
	batch := &wal.Batch{}
	batch.Delete(key)
	if err := m.wal.Submit(batch.Encode()); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for {
		err := m.active.Delete(key)
		if err == nil {
			if m.active.ShouldFlush() && m.immutable == nil {
				m.active.Freeze()
				m.immutable = m.active
				m.active = NewMemTable(m.maxSize)
				select {
				case m.flushCh <- struct{}{}:
				default:
				}
			}
			return nil
		}

		if err == ErrMemTableFull {
			if m.immutable != nil {
				m.cond.Wait()
				continue
			}
			m.active.Freeze()
			m.immutable = m.active
			m.active = NewMemTable(m.maxSize)
			select {
			case m.flushCh <- struct{}{}:
			default:
			}
			continue
		}

		return err
	}
}

func (m *Manager) flushLoop() {
	defer m.flushWg.Done()
	for {
		select {
		case <-m.flushCh:
			m.doFlush()
		case <-m.stopCh:
			m.doFlush()
			return
		}
	}
}

func (m *Manager) doFlush() {
	m.mu.RLock()
	imm := m.immutable
	m.mu.RUnlock()

	if imm == nil {
		return
	}

	if m.flushFn != nil {
		for {
			if err := m.flushFn(imm); err != nil {
				log.Printf("flush failed, retrying: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}
	}

	m.mu.Lock()
	m.immutable = nil
	m.cond.Broadcast()
	m.mu.Unlock()
}

func (m *Manager) ApplyRecoveredBatch(batch *wal.Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, entry := range batch.Entries {
		for {
			var err error
			if entry.Op == wal.OpPut {
				err = m.active.Put(entry.Key, entry.Value)
			} else {
				err = m.active.Delete(entry.Key)
			}

			if err == nil {
				if m.active.ShouldFlush() && m.immutable == nil {
					m.active.Freeze()
					m.immutable = m.active
					m.active = NewMemTable(m.maxSize)
					select {
					case m.flushCh <- struct{}{}:
					default:
					}
				}
				break
			}

			if err == ErrMemTableFull {
				if m.immutable != nil {
					m.cond.Wait()
					continue
				}
				m.active.Freeze()
				m.immutable = m.active
				m.active = NewMemTable(m.maxSize)
				select {
				case m.flushCh <- struct{}{}:
				default:
				}
				continue
			}

			return err
		}
	}
	return nil
}
