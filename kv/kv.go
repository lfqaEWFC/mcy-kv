package kv

import (
	"fmt"
	"sync"
	"time"

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
	go func() {
		fmt.Printf("sleeping...")
		time.Sleep(10 * time.Second)
		fmt.Printf("awake!")
		kv.rf.Start("hello")
	}()

	return kv
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		s, ok := msg.Command.(string)
		if !ok {
			panic("unexpected command type")
		}

		fmt.Println("[apply string]", s)
	}
}
