package kv

import (
	"fmt"
	"sync"
	"time"

	"mcy-kv/labgob"
	"mcy-kv/raftapi"
)

type Op struct {
	Type  string
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	applyCh chan raftapi.ApplyMsg
	kv      map[string]string
	rf      raftapi.Raft
}

func init() {
	labgob.Register(Op{})
}

func NewServer(rf raftapi.Raft, applyCh chan raftapi.ApplyMsg) *KVServer {
	kv := &KVServer{
		rf:      rf,
		applyCh: applyCh,
		kv:      make(map[string]string),
	}

	go kv.applier()
	go func() {
		fmt.Printf("sleeping...")
		time.Sleep(10 * time.Second)
		fmt.Printf("awake!")
		kv.rf.Start(Op{
			Type:  "Put",
			Key:   "k1",
			Value: "v1",
		})
	}()

	return kv
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			panic("unexpected command type (Op expected)")
		}

		fmt.Printf("[apply op] type=%s key=%s value=%s\n",
			op.Type, op.Key, op.Value)

		kv.mu.Lock()
		if op.Type == "Put" {
			kv.kv[op.Key] = op.Value
		}
		kv.mu.Unlock()
	}
}
