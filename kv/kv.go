package kv

import (
	"bytes"
	"fmt"
	"mcy-kv/labgob"
	"mcy-kv/raftapi"
	"sync"
	"time"
)

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

type Op struct {
	Type     string
	Key      string
	Value    string
	ClientID int64
	Seq      int
}

type OpResult struct {
	Err      Err
	Value    string
	ClientID int64
	Seq      int
}

type PutArgs struct {
	Key      string
	Value    string
	ClientID int64
	Seq      int
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientID int64
	Seq      int
}

type GetReply struct {
	Err   Err
	Value string
}

type KVServer struct {
	lastApplied  int
	maxraftstate int
	mu           sync.Mutex
	applyCh      chan raftapi.ApplyMsg
	kv           map[string]string
	rf           raftapi.Raft
	waitCh       map[int]chan OpResult
	lastseq      map[int64]int
	lastcmd      map[int64]OpResult
}

func init() {
	labgob.Register(Op{})
}

func NewServer(rf raftapi.Raft, applyCh chan raftapi.ApplyMsg, maxraftstate int) *KVServer {
	kv := &KVServer{
		rf:           rf,
		applyCh:      applyCh,
		kv:           make(map[string]string),
		lastseq:      make(map[int64]int),
		waitCh:       make(map[int]chan OpResult),
		lastcmd:      make(map[int64]OpResult),
		maxraftstate: maxraftstate,
	}

	go kv.applier()

	return kv
}

func (kv *KVServer) getWaitCh(index int) chan OpResult {
	ch, ok := kv.waitCh[index]
	if !ok {
		ch = make(chan OpResult, 1)
		kv.waitCh[index] = ch
	}
	return ch
}

func (kv *KVServer) removeWaitCh(index int) {
	delete(kv.waitCh, index)
}

func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	var data struct {
		KV      map[string]string
		LastSeq map[int64]int
		LastCmd map[int64]OpResult
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&data); err != nil {
		panic(fmt.Sprintf("KVServer restoreFromSnapshot decode failed: %v", err))
	}
	kv.kv = data.KV
	kv.lastseq = data.LastSeq
	kv.lastcmd = data.LastCmd
}

func (kv *KVServer) maybeTakeSnapshot() {
	if kv.maxraftstate <= 0 {
		return
	}

	kv.mu.Lock()
	fmt.Printf("PersisterBytes: %d\n", kv.rf.PersistBytes())
	if kv.rf.PersistBytes() < kv.maxraftstate {
		kv.mu.Unlock()
		return
	}
	kvCopyKV := make(map[string]string, len(kv.kv))
	for k, v := range kv.kv {
		kvCopyKV[k] = v
	}
	kvCopySeq := make(map[int64]int, len(kv.lastseq))
	for cid, seq := range kv.lastseq {
		kvCopySeq[cid] = seq
	}
	kvCopyCmd := make(map[int64]OpResult, len(kv.lastcmd))
	for cid, cmd := range kv.lastcmd {
		kvCopyCmd[cid] = cmd
	}
	lastApplied := kv.lastApplied
	kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(struct {
		KV      map[string]string
		LastSeq map[int64]int
		LastCmd map[int64]OpResult
	}{
		KV:      kvCopyKV,
		LastSeq: kvCopySeq,
		LastCmd: kvCopyCmd,
	}); err != nil {
		panic(fmt.Sprintf("KVServer maybeTakeSnapshot encode failed: %v", err))
	}
	snapshotBytes := w.Bytes()

	kv.rf.Snapshot(lastApplied, snapshotBytes)
	kv.mu.Lock()
	for idx := range kv.waitCh {
		if idx <= lastApplied {
			delete(kv.waitCh, idx)
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		fmt.Printf("index: %d\n", msg.CommandIndex)
		if msg.SnapshotValid {
			kv.mu.Lock()
			kv.restoreFromSnapshot(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			kv.mu.Unlock()
			fmt.Printf("snapshot valid...\n")
			continue
		}
		if !msg.CommandValid {
			fmt.Printf("error command type...\n")
			continue
		}
		fmt.Printf("command valid...\n")
		kv.mu.Lock()
		switch op := msg.Command.(type) {
		case Op:
			last := kv.lastseq[op.ClientID]
			if op.Seq > last {
				fmt.Printf("command type: %s\n", op.Type)
				switch op.Type {
				case "Put":
					fmt.Printf("Put command: key=%s, value=%s\n", op.Key, op.Value)
					kv.kv[op.Key] = op.Value
				case "Get":
					fmt.Printf("Get operation applied for key: %s\n", op.Key)
				default:
					fmt.Printf("Unknown operation type: %s\n", op.Type)
					panic("KVServer: unknown operation type\n")
				}
				kv.lastseq[op.ClientID] = op.Seq
			}
			res := OpResult{
				Err:      OK,
				ClientID: op.ClientID,
				Seq:      op.Seq,
			}
			if op.Type == "Get" {
				res.Value = kv.kv[op.Key]
				kv.lastcmd[op.ClientID] = res
			}
			if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
				select {
				case ch <- res:
				default:
				}
			}
		case raftapi.NoOp:
			fmt.Printf("No-op command applied\n")
		default:
			fmt.Printf("Unknown command type: %T\n", msg.Command)
			panic("KVServer: unknown command type\n")
		}
		kv.lastApplied = msg.CommandIndex
		kv.mu.Unlock()
		kv.maybeTakeSnapshot()
	}
}

func (kv *KVServer) Put(args PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	if kv.lastseq[args.ClientID] >= args.Seq {
		reply.Err = OK
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()
	op := Op{
		Type:     "Put",
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	kv.mu.Lock()
	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		kv.removeWaitCh(index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		if res.ClientID != op.ClientID || res.Seq != op.Seq {
			reply.Err = ErrWrongLeader
			return nil
		}
		reply.Err = res.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	return nil
}

func (kv *KVServer) Get(args GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	if kv.lastseq[args.ClientID] >= args.Seq {
		reply.Err = OK
		reply.Value = kv.lastcmd[args.ClientID].Value
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}
	kv.mu.Lock()
	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		kv.removeWaitCh(index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		if res.ClientID != op.ClientID || res.Seq != op.Seq {
			reply.Err = ErrWrongLeader
			return nil
		}
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	return nil
}
