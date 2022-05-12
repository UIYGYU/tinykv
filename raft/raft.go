// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

var globalrand = rand.New(rand.NewSource(time.Now().UnixNano()))

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	//votes2 is whether vote or not
	votes2 map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed           int
	randomizedElectionTimeout int
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	peers := c.peers
	votes := map[uint64]bool{}
	votes2 := map[uint64]bool{}
	for _, v := range peers {
		votes[v] = false
	}
	for _, v := range peers {
		votes2[v] = false
	}
	r := &Raft{
		id:                        c.ID,
		Term:                      0,
		Vote:                      None,
		Lead:                      None,
		RaftLog:                   newLog(c.Storage),
		Prs:                       map[uint64]*Progress{},
		State:                     StateFollower,
		votes:                     votes,
		votes2:                    votes2,
		msgs:                      []pb.Message{},
		heartbeatElapsed:          0,
		electionElapsed:           0,
		heartbeatTimeout:          c.HeartbeatTick,
		electionTimeout:           c.ElectionTick,
		randomizedElectionTimeout: c.ElectionTick + globalrand.Intn(c.ElectionTick),
		leadTransferee:            None,
		PendingConfIndex:          None,
	}
	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1}
	}
	hs, _, _ := c.Storage.InitialState()
	if !isHardStateEqual(hs, pb.HardState{}) {
		r.RaftLog.committed = hs.Commit
		r.Vote = hs.Vote
		r.Term = hs.Term
	}
	return r
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	if pr.Next <= r.RaftLog.LastIndex() {
		term, _ := r.RaftLog.Term(pr.Next - 1)
		index := pr.Next - 1
		li, _ := r.RaftLog.storage.FirstIndex()
		var ents []*pb.Entry
		for i := pr.Next - li; i < uint64(len(r.RaftLog.entries)); i++ {
			ents = append(ents, &r.RaftLog.entries[i])
		}
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term, LogTerm: term, Index: index, Entries: ents, Commit: r.RaftLog.committed})
		return true
	}
	if pr.Match == r.RaftLog.LastIndex() {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term, Commit: r.RaftLog.committed})
		return true
	}
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, From: r.id, To: to, Term: r.Term, Commit: r.RaftLog.committed})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			//r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgBeat})
			for key, _ := range r.votes {
				if key != r.id {
					r.sendHeartbeat(key)
				}
			}
		}
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.randomizedElectionTimeout = r.electionTimeout + globalrand.Intn(r.electionTimeout)
			//r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHup})
			r.hup()
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.randomizedElectionTimeout = r.electionTimeout + globalrand.Intn(r.electionTimeout)
			r.hup()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = lead
	r.State = StateFollower
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + globalrand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Lead = None
	r.Vote = r.id
	r.State = StateCandidate
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + globalrand.Intn(r.electionTimeout)
	for key, _ := range r.votes {
		r.votes[key] = false
	}
	for key, _ := range r.votes2 {
		r.votes2[key] = false
	}
	r.votes[r.id] = true
	r.votes2[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.State = StateLeader
	for key, _ := range r.votes {
		if key != r.id {
			r.Prs[key] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
		} else {
			r.Prs[key] = &Progress{Match: r.RaftLog.LastIndex(), Next: r.RaftLog.LastIndex() + 1}
		}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1, Data: nil})
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.votes) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) hup() {
	r.becomeCandidate()
	if len(r.votes) == 1 {
		r.becomeLeader()
	} else {
		for key, value := range r.votes {
			if value == false {
				logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
				index := r.RaftLog.LastIndex()
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: key, Term: r.Term, LogTerm: logterm, Index: index})
			}
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
	case m.Term < r.Term:
		return nil
	case m.Term > r.Term:
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.hup()
		case pb.MessageType_MsgRequestVote:
			canVote := (r.Vote == None) || (r.Vote == m.From)
			lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
			if canVote && (m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex())) {
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term})
			} else {
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true})
			}
		case pb.MessageType_MsgHeartbeat:
			r.Lead = m.From
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleAppendEntries(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true})
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			r.votes2[m.From] = true
			i := 0
			for _, value := range r.votes {
				if value == true {
					i++
				}
			}
			if 2*i > len(r.votes) {
				r.becomeLeader()
				for key, _ := range r.votes {
					if key != r.id {
						r.sendAppend(key)
					}
				}
			}
			j := 0
			for key, value := range r.votes {
				v := r.votes2[key]
				if value == false && v == true {
					j++
				}
			}
			if 2*j > len(r.votes) {
				r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			r.hup()
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id, Term: r.Term, Reject: true})
		case pb.MessageType_MsgBeat:
			for key, _ := range r.votes {
				if key != r.id {
					r.sendHeartbeat(key)
				}
			}
		case pb.MessageType_MsgPropose:
			for _, v := range m.Entries {
				v.Term = r.Term
				v.Index = r.RaftLog.LastIndex() + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *v)
				r.Prs[r.id].Match = r.RaftLog.LastIndex()
				r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
				if len(r.votes) == 1 {
					r.RaftLog.committed = r.Prs[r.id].Match
				}
				for key, _ := range r.votes {
					if key != r.id {
						r.sendAppend(key)
					}
				}
			}
		case pb.MessageType_MsgAppendResponse:
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
			if m.Reject {
				r.sendAppend(m.From)
			} else {
				voteMap := make(map[uint64]uint64, 0)
				ks := []uint64{}
				for _, v := range r.Prs {
					voteMap[v.Match]++
				}
				for key, _ := range voteMap {
					ks = append(ks, key)
				}
				sort.Slice(ks, func(i, j int) bool { return ks[i] > ks[j] })
				var i uint64 = 0
				for _, index := range ks {
					i += voteMap[index]
					t, _ := r.RaftLog.Term(index)
					if 2*i > uint64(len(r.Prs)) && r.RaftLog.committed < index && t == r.Term {
						r.RaftLog.committed = index
						for to, _ := range r.votes {
							if to != r.id {
								r.sendAppend(to)
							}
						}
					}
				}
			}
		case pb.MessageType_MsgHeartbeatResponse:
			pr := r.Prs[m.From]
			if pr.Match < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if m.Index == 0 && m.LogTerm == 0 && m.Entries == nil && m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = m.Commit
		return
	}
	if m.Index < r.RaftLog.committed {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term, Index: r.RaftLog.committed})
		return
	}
	entries := make([]pb.Entry, 0)
	for _, v := range m.Entries {
		entries = append(entries, *v)
	}
	if matchindex, ok := r.RaftLog.maybeappend(m.Index, m.LogTerm, m.Commit, entries...); ok {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Index: matchindex})
	} else {
		hintIndex := min(m.Index, r.RaftLog.LastIndex())
		if hintIndex > r.RaftLog.committed {
			hintIndex--
		}
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id, Term: r.Term, Index: hintIndex, Reject: true})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, From: r.id, To: m.From})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
