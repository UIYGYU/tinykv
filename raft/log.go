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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	li, _ := storage.FirstIndex()
	r, _ := storage.LastIndex()
	hard, _, _ := storage.InitialState()
	ents1, _ := storage.Entries(li, r+1)
	l := &RaftLog{
		storage:   storage,
		entries:   ents1,
		committed: hard.Commit,
		applied:   li - 1,
		stabled:   r,
		offset:    li,
	}
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	hi := uint64(len(l.entries))
	if l.stabled < hi+l.offset-1 {
		return l.entries[l.stabled-l.offset+1:]
	} else {
		return []pb.Entry{}
	}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied < l.committed {
		ents = l.entries[l.applied-l.offset+1 : l.committed-l.offset+1]
	}
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var i uint64
	if len(l.entries) != 0 {
		i = l.entries[len(l.entries)-1].Index
	} else if l.pendingSnapshot != nil {
		i = l.pendingSnapshot.Metadata.Index
	} else {
		i = l.offset - 1
	}
	return i
}

func (l *RaftLog) FirstIndex() uint64 {
	var i uint64
	if len(l.entries) != 0 {
		i = l.entries[0].Index
	} else if l.pendingSnapshot != nil {
		i = l.pendingSnapshot.Metadata.Index
	} else {
		i = l.offset - 1
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	q, _ := l.storage.FirstIndex()
	lindex := q - 1
	if i < lindex || i > l.LastIndex() {
		return 0, ErrCompacted
	}
	if i <= l.stabled {
		return l.storage.Term(i)
	} else if i <= l.LastIndex() {
		return l.entries[i-q].Term, nil
	} else {
		return 0, ErrCompacted
	}
}

func (l *RaftLog) findconflict(ents []pb.Entry) uint64 {
	for _, en := range ents {
		if !l.match(en.Term, en.Index) {
			return en.Index
		}
	}
	return 0
}

func (l *RaftLog) maybeappend(index, term, committed uint64, ents ...pb.Entry) (uint64, bool) {
	if l.match(index, term) {
		lastidx := index + uint64(len(ents))
		conflict := l.findconflict(ents)
		switch {
		case conflict == 0:
		case conflict <= l.committed:
		default:
			li, _ := l.storage.FirstIndex()
			offset := index + 1
			l.entries = l.entries[:conflict-li]
			ents = ents[conflict-offset:]
			l.entries = append(l.entries, ents...)
			if conflict-1 < l.stabled {
				l.stabled = conflict - 1
			}
		}
		if l.committed <= min(lastidx, committed) {
			l.committed = min(lastidx, committed)
		}
		return lastidx, true
	}
	return 0, false
}

func (l *RaftLog) match(index, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}
	return term == t
}

//func (l *RaftLog) clostestmatch(index, term uint64) uint64 {
//	if r := l.LastIndex(); index > r {
//		return index
//	}
//	for {
//		logterm, err := l.Term(index)
//		if logterm <= term || err != nil {
//			break
//		}
//		index--
//	}
//	return index
//}
