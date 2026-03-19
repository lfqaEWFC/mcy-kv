package persister

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	file      string
	raftstate []byte
	snapshot  []byte
}

func MakePersister(name string, id int) *Persister {
	ps := &Persister{
		file: fmt.Sprintf("persist_%s_%d.bin", name, id),
	}
	data, err := os.ReadFile(ps.file)
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

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	x := make([]byte, len(b))
	copy(x, b)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	np := &Persister{
		file:      ps.file,
		raftstate: clone(ps.raftstate),
		snapshot:  clone(ps.snapshot),
	}

	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

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

	tmpFile := ps.file + ".tmp"
	os.WriteFile(tmpFile, buf.Bytes(), 0644)

	os.Rename(tmpFile, ps.file)
}
