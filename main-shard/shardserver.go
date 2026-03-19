package main

import (
	"flag"
	"fmt"
	"math/rand"
	"mcy-kv/ctrlerclient"
	persister "mcy-kv/labpersister"
	"mcy-kv/raft"
	"mcy-kv/raftapi"
	"mcy-kv/shardkv"
	"mcy-kv/transport"
)

func main() {
	ctrlerpeers := map[int]string{
		0: "127.0.0.1:8016",
		1: "127.0.0.1:8017",
		2: "127.0.0.1:8018",
		3: "127.0.0.1:8019",
		4: "127.0.0.1:8020",
	}
	clientID := rand.Int63()
	fmt.Printf("newclient start\n")
	ck := ctrlerclient.NewClient(ctrlerpeers, clientID)
	fmt.Printf("newclient end\n")
	fmt.Printf("query start\n")
	config := ck.Query(-1)
	fmt.Printf("query end\n")
	shardpeers := config.Groups
	id := flag.Int("id", -1, "server id")
	flag.Parse()
	if *id < 0 {
		panic("must specify --id")
	}
	me := *id
	for gid, peers := range shardpeers {
		applyCh := make(chan raftapi.ApplyMsg, 1000)
		file := fmt.Sprintf("shardserver_gid_%d_id", gid)
		ps := persister.MakePersister(file, me)
		t := transport.NewHTTPTransport(
			me,
			peers,
		)
		rf := raft.Make(
			peers,
			me,
			t,
			ps,
			applyCh,
		)
		clientID := rand.Int63()
		ck := ctrlerclient.NewClient(ctrlerpeers, clientID)
		kvsrv := shardkv.NewServer(rf, applyCh, 1000, ck, gid)
		t.Register(rf)
		t.Register(kvsrv)
		if err := t.Start(); err != nil {
			panic(err)
		}
		fmt.Printf("ShardKV server %d gid %d ", me, gid)
		fmt.Printf("clientID %d listening on %s\n", clientID, peers[me])
	}
	//阻塞于此，保持服务器运行
	select {}
}
