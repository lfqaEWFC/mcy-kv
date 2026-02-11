package main

import (
	"fmt"
	"time"

	persister "mcy-kv/labpersister"
	"mcy-kv/labrpc"
	"mcy-kv/raft"
	"mcy-kv/raftapi"
)

func main() {
	numPeers := 3
	applyChs := make([]chan raftapi.ApplyMsg, numPeers)

	// 创建虚拟网络
	net := labrpc.MakeNetwork()

	persisters := make([]*persister.Persister, numPeers)
	peers := make([]*labrpc.ClientEnd, numPeers)
	rfs := make([]raftapi.Raft, numPeers)
	servers := make([]*labrpc.Server, numPeers)

	for i := 0; i < numPeers; i++ {
		// Persister
		persisters[i] = persister.MakePersister()

		// 创建 ClientEnd
		peers[i] = net.MakeEnd(fmt.Sprintf("peer%d", i))

		// 创建 Server 并注册 Raft 服务
		applyChs[i] = make(chan raftapi.ApplyMsg, 100)
		rf := raft.Make(peers, i, persisters[i], applyChs[i]).(*raft.Raft)
		rfs[i] = rf
		servers[i] = labrpc.MakeServer()
		servers[i].AddService(labrpc.MakeService(rf))

		// 把 ClientEnd 连接到 Server 并启用
		net.AddServer(fmt.Sprintf("peer%d", i), servers[i])
		net.Connect(fmt.Sprintf("peer%d", i), fmt.Sprintf("peer%d", i))
		net.Enable(fmt.Sprintf("peer%d", i), true)
	}

	// 提交命令测试
	go func() {
		for {
			for i := 0; i < numPeers; i++ {
				time.Sleep(500 * time.Millisecond)
				index, term, isLeader := rfs[i].Start(fmt.Sprintf("cmd at %v", time.Now()))
				term, leader := rfs[i].GetState()
				fmt.Printf("is [Leader] : %v, term=%d\n", leader, term)
				if isLeader {
					fmt.Printf("[Leader] 提交命令 index=%d term=%d\n", index, term)
				}
			}
		}
	}()

	// 接收 ApplyMsg
	for msg := range applyChs[0] {
		fmt.Println("applychs[0]:\n", msg)
	}
}
