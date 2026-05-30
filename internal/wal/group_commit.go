package wal

import (
	"time"
)

type GroupCommitOption func(*GroupCommitter)

func WithMaxBatchSize(size int) GroupCommitOption {
	return func(gc *GroupCommitter) {
		gc.maxBatchSize = size
	}
}

func WithMaxBatchDelay(delay time.Duration) GroupCommitOption {
	return func(gc *GroupCommitter) {
		gc.maxBatchDelay = delay
	}
}

type commitRequest struct {
	data   []byte
	result chan error
}

type GroupCommitter struct {
	writer        *Writer
	pending       chan commitRequest
	stopCh        chan struct{}
	stoppedCh     chan struct{}
	maxBatchSize  int
	maxBatchDelay time.Duration
}

func NewGroupCommitter(writer *Writer, opts ...GroupCommitOption) *GroupCommitter {
	gc := &GroupCommitter{
		writer:        writer,
		pending:       make(chan commitRequest, 1024),
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
		maxBatchSize:  64,
		maxBatchDelay: 1 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(gc)
	}
	return gc
}

func (gc *GroupCommitter) Start() {
	go gc.run()
}

func (gc *GroupCommitter) Submit(data []byte) error {
	req := commitRequest{
		data:   data,
		result: make(chan error, 1),
	}
	select {
	case gc.pending <- req:
	case <-gc.stopCh:
		return ErrClosed
	}
	return <-req.result
}

func (gc *GroupCommitter) Stop() {
	close(gc.stopCh)
	<-gc.stoppedCh
}

func (gc *GroupCommitter) run() {
	defer close(gc.stoppedCh)

	var batch []commitRequest
	timer := time.NewTimer(gc.maxBatchDelay)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timerActive := false

	flush := func() {
		if len(batch) == 0 {
			return
		}

		var err error
		for _, req := range batch {
			if appendErr := gc.writer.Append(req.data); appendErr != nil {
				err = appendErr
				break
			}
		}

		if err == nil {
			err = gc.writer.Sync()
		}

		for _, req := range batch {
			req.result <- err
		}

		batch = batch[:0]
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerActive = false
		}
	}

	for {
		select {
		case req := <-gc.pending:
			batch = append(batch, req)
			if len(batch) >= gc.maxBatchSize {
				flush()
			} else if !timerActive {
				timer.Reset(gc.maxBatchDelay)
				timerActive = true
			}
		case <-timer.C:
			timerActive = false
			flush()
		case <-gc.stopCh:
			// Drain remaining
			for {
				select {
				case req := <-gc.pending:
					batch = append(batch, req)
				default:
					flush()
					return
				}
			}
		}
	}
}
