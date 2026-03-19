package main

import (
	"flag"
	"fmt"
	"mcy-kv/labgob"
	persister "mcy-kv/labpersister"
	"mcy-kv/raft"
	"mcy-kv/raftapi"
	"mcy-kv/shardctrler"
	"mcy-kv/transport"
)

func main() {
	peers := map[int]string{
		0: "127.0.0.1:8016",
		1: "127.0.0.1:8017",
		2: "127.0.0.1:8018",
		3: "127.0.0.1:8019",
		4: "127.0.0.1:8020",
	}

	id := flag.Int("id", -1, "server id")
	flag.Parse()
	if *id < 0 {
		panic("must specify --id")
	}
	me := *id

	applyCh := make(chan raftapi.ApplyMsg, 1000)

	ps := persister.MakePersister("shardctrler", me)

	t := transport.NewHTTPTransport(
		me,
		peers,
	)

	fmt.Printf("labgob register\n")
	labgob.Register(shardctrler.Op{})
	labgob.Register(shardctrler.Config{})

	fmt.Printf("make start\n")
	rf := raft.Make(
		peers,
		me,
		t,
		ps,
		applyCh,
	)

	shardctrler := shardctrler.NewServer(rf, applyCh, 1000)

	t.Register(rf)
	t.Register(shardctrler)

	if err := t.Start(); err != nil {
		panic(err)
	}

	fmt.Printf("ShardCtrler server %d listening on %s\n", me, peers[me])
	select {}
}
