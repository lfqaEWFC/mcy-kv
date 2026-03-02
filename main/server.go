package main

import (
	"fmt"
	"time"

	persister "mcy-kv/labpersister"
	"mcy-kv/raft"
	"mcy-kv/raftapi"
	"mcy-kv/transport"
)

func main() {
	numPeers := 5

	peers := map[int]string{
		0: "127.0.0.1:8001",
		1: "127.0.0.1:8002",
		2: "127.0.0.1:8003",
		3: "127.0.0.1:8004",
		4: "127.0.0.1:8005",
	}

	rfs := make([]raftapi.Raft, numPeers)
	applyChs := make([]chan raftapi.ApplyMsg, numPeers)

	for i := 0; i < numPeers; i++ {
		applyChs[i] = make(chan raftapi.ApplyMsg, 100)

		persister := persister.MakePersister()

		t := transport.NewHTTPTransport(
			i,
			peers[i],
			peers,
		)

		rf := raft.Make(
			peers,
			i,
			t,
			persister,
			applyChs[i],
		)

		// 注册 Raft RPC
		t.Register(rf)

		// 启动监听
		if err := t.Start(); err != nil {
			panic(err)
		}

		rfs[i] = rf
	}

	// 模拟客户端不断提交
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			for i := 0; i < numPeers; i++ {
				index, term, isLeader := rfs[i].Start(
					fmt.Sprintf("cmd at %v", time.Now()),
				)
				if isLeader {
					fmt.Printf(
						"[Leader %d] submit index=%d term=%d\n",
						i, index, term,
					)
				}
			}
		}
	}()

	// 观察 apply
	go func() {
		for msg := range applyChs[0] {
			fmt.Println("[apply 0]", msg)
		}
	}()

	select {}
}
