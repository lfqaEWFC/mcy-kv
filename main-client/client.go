package main

import (
	"mcy-kv/kvclient"
)

func main() {

	servers := map[int]string{
		0: "127.0.0.1:8001",
		1: "127.0.0.1:8002",
		2: "127.0.0.1:8003",
		3: "127.0.0.1:8004",
		4: "127.0.0.1:8005",
	}

	ck := kvclient.MakeClerk(servers)
	ck.Put("x", "test")
}
