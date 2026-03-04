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
		fmt.Printf("sleeping...\n")
		time.Sleep(10 * time.Second)
		fmt.Printf("awake!\n")
		kv.rf.Start(Op{
			Type:  "Put",
			Key:   "k1",
			Value: "v1",
		})
	}()

	return kv
}

func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {

}

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
		switch op.Type {
		case "Put":
			fmt.Printf("Put command: key=%s, value=%s\n", op.Key, op.Value)
			kv.kv[op.Key] = op.Value
		case "Append":
			kv.kv[op.Key] += op.Value
		case "Get":
			fmt.Printf("Get operation applied for key: %s\n", op.Key)
		default:
			panic("KVServer: unknown op type\n")
		}
		kv.mu.Unlock()
	}
}
