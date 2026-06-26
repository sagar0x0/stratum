package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	pb "github.com/sagar0x0/stratum/proto/raft"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Config holds Raft configuration.
type Config struct {
	ID             string
	Peers          []string
	ElectionMin    time.Duration
	ElectionMax    time.Duration
	HeartbeatTime  time.Duration
	LeaseDuration  time.Duration
}

// Node represents a Raft node.
type Node struct {
	mu sync.RWMutex

	id    string
	peers []string

	state       State
	currentTerm uint64
	votedFor    string
	
	// Leader Lease
	leaseTimeout time.Time

	commitIndex uint64
	lastApplied uint64

	// Channels for state machine
	applyCh chan *pb.LogEntry

	// Timers
	electionTimer *time.Timer
	heartbeatTimer *time.Timer

	// Transport
	transport Transport

	// WAL for persistence
	wal *WAL
	
	// In-memory log (in a real system, backed by WAL)
	log []*pb.LogEntry
	
	// Leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	
	stopCh chan struct{}
}

// Transport defines how a node sends RPCs to other nodes.
type Transport interface {
	RequestVote(ctx context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

func NewNode(cfg Config, transport Transport, walDir string, applyCh chan *pb.LogEntry) (*Node, error) {
	wal, err := OpenWAL(walDir)
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:          cfg.ID,
		peers:       cfg.Peers,
		state:       Follower,
		applyCh:     applyCh,
		transport:   transport,
		wal:         wal,
		log:         make([]*pb.LogEntry, 0),
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		stopCh:      make(chan struct{}),
	}
	
	// Dummy entry to make log 1-indexed
	n.log = append(n.log, &pb.LogEntry{Term: 0, Index: 0})
	
	n.electionTimer = time.NewTimer(randomDuration(cfg.ElectionMin, cfg.ElectionMax))
	n.heartbeatTimer = time.NewTimer(cfg.HeartbeatTime)
	n.heartbeatTimer.Stop() // Only leader runs heartbeat timer

	go n.loop(cfg)

	return n, nil
}

func (n *Node) loop(cfg Config) {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.startElection(cfg)
		case <-n.heartbeatTimer.C:
			n.sendHeartbeats(cfg)
		}
	}
}

func (n *Node) startElection(cfg Config) {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	
	// Persist state
	n.wal.SaveState(n.currentTerm, n.votedFor)

	term := n.currentTerm
	lastLogIdx := uint64(len(n.log) - 1)
	lastLogTerm := n.log[lastLogIdx].Term
	n.mu.Unlock()

	req := &pb.RequestVoteRequest{
		Term:         term,
		CandidateId:  n.id,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}

	votes := 1
	var votesMu sync.Mutex

	for _, peer := range cfg.Peers {
		if peer == n.id {
			continue
		}
		go func(p string) {
			ctx, cancel := context.WithTimeout(context.Background(), cfg.HeartbeatTime)
			defer cancel()
			
			resp, err := n.transport.RequestVote(ctx, p, req)
			if err != nil {
				return
			}
			
			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if resp.Term > n.currentTerm {
				n.stepDown(resp.Term)
				return
			}

			if resp.VoteGranted {
				votesMu.Lock()
				votes++
				if votes > (len(cfg.Peers)+1)/2 {
					n.becomeLeader(cfg)
				}
				votesMu.Unlock()
			}
		}(peer)
	}
	
	n.resetElectionTimer(cfg)
}

func (n *Node) becomeLeader(cfg Config) {
	n.state = Leader
	for _, p := range n.peers {
		n.nextIndex[p] = uint64(len(n.log))
		n.matchIndex[p] = 0
	}
	n.electionTimer.Stop()
	n.heartbeatTimer.Reset(cfg.HeartbeatTime)
	n.sendHeartbeats(cfg)
}

func (n *Node) stepDown(term uint64) {
	n.currentTerm = term
	n.state = Follower
	n.votedFor = ""
	n.wal.SaveState(n.currentTerm, n.votedFor)
	n.heartbeatTimer.Stop()
}

func (n *Node) resetElectionTimer(cfg Config) {
	n.electionTimer.Stop()
	n.electionTimer.Reset(randomDuration(cfg.ElectionMin, cfg.ElectionMax))
}

func (n *Node) sendHeartbeats(cfg Config) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderId := n.id
	n.mu.Unlock()

	var successCount int = 1
	var countMu sync.Mutex

	for _, peer := range cfg.Peers {
		if peer == n.id {
			continue
		}
		go func(p string) {
			n.mu.RLock()
			nextIdx := n.nextIndex[p]
			prevLogIdx := nextIdx - 1
			prevLogTerm := n.log[prevLogIdx].Term
			entries := n.log[nextIdx:]
			commitIdx := n.commitIndex
			n.mu.RUnlock()

			req := &pb.AppendEntriesRequest{
				Term:         term,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIdx,
			}

			ctx, cancel := context.WithTimeout(context.Background(), cfg.HeartbeatTime)
			defer cancel()

			resp, err := n.transport.AppendEntries(ctx, p, req)
			if err != nil {
				return
			}
			
			countMu.Lock()
			successCount++
			if successCount > (len(cfg.Peers)+1)/2 {
				n.mu.Lock()
				// Extend lease
				n.leaseTimeout = time.Now().Add(cfg.LeaseDuration)
				n.mu.Unlock()
			}
			countMu.Unlock()

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Leader || n.currentTerm != term {
				return
			}

			if resp.Term > n.currentTerm {
				n.stepDown(resp.Term)
				n.resetElectionTimer(cfg)
				return
			}

			if resp.Success {
				n.matchIndex[p] = prevLogIdx + uint64(len(entries))
				n.nextIndex[p] = n.matchIndex[p] + 1
				n.advanceCommitIndex()
			} else {
				if n.nextIndex[p] > 1 {
					n.nextIndex[p]--
				}
			}
		}(peer)
	}
	
	n.heartbeatTimer.Reset(cfg.HeartbeatTime)
}

func (n *Node) advanceCommitIndex() {
	// Not fully implemented O(N log N) median finding, simplified:
	for i := len(n.log) - 1; i > int(n.commitIndex); i-- {
		count := 1
		for _, p := range n.peers {
			if p != n.id && n.matchIndex[p] >= uint64(i) {
				count++
			}
		}
		if count > (len(n.peers)+1)/2 && n.log[i].Term == n.currentTerm {
			n.commitIndex = uint64(i)
			n.applyLog()
			break
		}
	}
}

func (n *Node) applyLog() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		n.applyCh <- entry
	}
}

func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	// A bit simplified...
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	}

	resp := &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}

	if req.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == req.CandidateId) {
		lastLogIdx := uint64(len(n.log) - 1)
		lastLogTerm := n.log[lastLogIdx].Term

		if req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIdx) {
			n.votedFor = req.CandidateId
			n.wal.SaveState(n.currentTerm, n.votedFor)
			resp.VoteGranted = true
		}
	}

	return resp, nil
}

func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
	}

	resp := &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false}

	if req.Term < n.currentTerm {
		return resp, nil
	}

	// We got a valid heartbeat from leader, reset election timer
	n.state = Follower
	// TODO: n.resetElectionTimer(cfg) - need access to cfg here or use a channel
	
	if req.PrevLogIndex >= uint64(len(n.log)) || n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		return resp, nil
	}

	// Append entries
	n.log = append(n.log[:req.PrevLogIndex+1], req.Entries...)
	n.wal.AppendEntries(req.Entries)

	if req.LeaderCommit > n.commitIndex {
		if req.LeaderCommit < uint64(len(n.log)-1) {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = uint64(len(n.log) - 1)
		}
		n.applyLog()
	}

	resp.Success = true
	return resp, nil
}

func randomDuration(min, max time.Duration) time.Duration {
	return min + time.Duration(rand.Int63n(int64(max-min)))
}
