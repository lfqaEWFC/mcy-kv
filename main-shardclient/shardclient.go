package main

import (
	"fmt"
	"math/rand"
	"mcy-kv/ctrlerclient"
	"mcy-kv/shardkvclient"
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
	ctrlerck := ctrlerclient.NewClient(ctrlerpeers, clientID)
	fmt.Printf("newclient end\n")
	ck := shardkvclient.MakeClerk(ctrlerck)
	fmt.Printf("makeclerk end\n")
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
