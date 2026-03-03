package main

import (
	"flag"
	"fmt"

	"mcy-kv/kv"
	persister "mcy-kv/labpersister"
	"mcy-kv/raft"
	"mcy-kv/raftapi"
	"mcy-kv/transport"
)

func main() {
	peers := map[int]string{
		0: "10.49.114.181:8001",
		1: "10.49.114.181:8002",
		2: "10.49.114.181:8003",
		3: "10.49.114.181:8004",
		4: "10.49.114.181:8005",
	}

	id := flag.Int("id", -1, "server id")
	flag.Parse()
	if *id < 0 {
		panic("must specify --id")
	}
	me := *id

	applyCh := make(chan raftapi.ApplyMsg, 100)

	persister := persister.MakePersister()

	t := transport.NewHTTPTransport(
		me,
		peers,
	)

	rf := raft.Make(
		peers,
		me,
		t,
		persister,
		applyCh,
	)

	kvsrv := kv.NewServer(rf, applyCh)

	t.Register(rf)
	t.Register(kvsrv)

	if err := t.Start(); err != nil {
		panic(err)
	}

	fmt.Printf("KV Raft server %d listening on %s\n", me, peers[me])

	select {}
}
