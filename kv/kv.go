package kv

import (
	"fmt"
	"sync"

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

func NewServer(rf raftapi.Raft, applyCh chan raftapi.ApplyMsg) *KVServer {
	kv := &KVServer{
		rf:      rf,
		applyCh: applyCh,
		kv:      make(map[string]string),
	}

	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)

			kv.mu.Lock()
			switch op.Type {
			case "Put":
				kv.kv[op.Key] = op.Value
				fmt.Printf("[apply] %s=%s\n", op.Key, op.Value)
			}
			kv.mu.Unlock()
		}
	}
}
