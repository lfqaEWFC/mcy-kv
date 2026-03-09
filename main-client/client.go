package main

import (
	"fmt"
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
	for {
		fmt.Printf("Enter command (get <key> or put <key> <value>): ")
		var cmd, key, value string
		fmt.Scanln(&cmd, &key, &value)
		switch cmd {
		case "get":
			ck.Get(key)
		case "put":
			if value == "" {
				fmt.Println("value is required for put command")
				continue
			}
			ck.Put(key, value)
		default:
			fmt.Println("unknown command")
		}
	}
}
