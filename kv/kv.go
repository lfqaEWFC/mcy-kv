package kv

import (
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

type KVServer struct {
	lastApplied  int
	maxraftstate int
	mu           sync.Mutex
	applyCh      chan raftapi.ApplyMsg
	kv           map[string]string
	rf           raftapi.Raft
	waitCh       map[int]chan OpResult
	lastseq      map[int64]int
}

func init() {
	labgob.Register(Op{})
}

func NewServer(rf raftapi.Raft, applyCh chan raftapi.ApplyMsg) *KVServer {
	kv := &KVServer{
		rf:      rf,
		applyCh: applyCh,
		kv:      make(map[string]string),
		lastseq: make(map[int64]int),
	}

	go kv.applier()

	return kv
}

func (kv *KVServer) getWaitCh(index int) chan OpResult {
	if kv.waitCh == nil {
		kv.waitCh = make(map[int]chan OpResult)
	}
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

func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.SnapshotValid {
			kv.mu.Lock()
			kv.restoreFromSnapshot(msg.Snapshot)
			kv.mu.Unlock()
			fmt.Printf("snapshot valid...\n")
			continue
		}
		if !msg.CommandValid {
			fmt.Printf("error command type...\n")
			continue
		}
		fmt.Printf("command valid...\n")
		op, ok := msg.Command.(Op)
		if !ok {
			panic("KVServer: unexpected command type\n")
		}
		kv.mu.Lock()
		if last, ok := kv.lastseq[op.ClientID]; !ok || op.Seq > last {
			switch op.Type {
			case "Put":
				fmt.Printf("Put command: key=%s, value=%s\n", op.Key, op.Value)
				kv.kv[op.Key] = op.Value
			case "Append":
				kv.kv[op.Key] += op.Value
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
		}
		if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
			select {
			case ch <- res:
			default:
			}
		}
		kv.lastApplied = msg.CommandIndex
		kv.mu.Unlock()
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
