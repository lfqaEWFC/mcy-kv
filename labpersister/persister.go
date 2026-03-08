package persister

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
)

const persistFile = "persist_state.bin"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	ps := &Persister{}
	data, err := os.ReadFile(persistFile)
	if err == nil && len(data) > 0 {
		buf := bytes.NewBuffer(data)
		var raftLen int64
		var snapLen int64
		binary.Read(buf, binary.LittleEndian, &raftLen)
		raft := make([]byte, raftLen)
		buf.Read(raft)
		binary.Read(buf, binary.LittleEndian, &snapLen)
		snap := make([]byte, snapLen)
		buf.Read(snap)
		ps.raftstate = raft
		ps.snapshot = snap
	}
	return ps
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int64(len(ps.raftstate)))
	buf.Write(ps.raftstate)
	binary.Write(buf, binary.LittleEndian, int64(len(ps.snapshot)))
	buf.Write(ps.snapshot)
	os.WriteFile(persistFile, buf.Bytes(), 0644)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
