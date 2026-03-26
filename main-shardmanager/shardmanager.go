package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"mcy-kv/ctrlerclient"
	"os"
	"strconv"
	"strings"
)

const NShards = 10
const Groups = 3

func main() {
	peers := map[int]string{
		0: "127.0.0.1:8016",
		1: "127.0.0.1:8017",
		2: "127.0.0.1:8018",
		3: "127.0.0.1:8019",
		4: "127.0.0.1:8020",
	}

	fmt.Println("ShardKV Manager: move command interface")
	scanner := bufio.NewScanner(os.Stdin)
	clientID := rand.Int63()
	ck := ctrlerclient.NewClient(peers, clientID)

	for {
		fmt.Print("cmd> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		args := strings.Fields(line)
		cmd := strings.ToLower(args[0])

		switch cmd {
		case "exit":
			return

		case "move":
			if len(args) != 3 {
				fmt.Println("usage: move <shard> <targetGID>")
				continue
			}
			shard, err1 := strconv.Atoi(args[1])
			targetGID, err2 := strconv.Atoi(args[2])
			if err1 != nil || err2 != nil {
				fmt.Println("shard and targetGID must be integers")
				continue
			}
			if shard < 0 || shard >= NShards {
				fmt.Println("shard index out of range")
				continue
			}
			if targetGID <= 0 || targetGID > Groups {
				fmt.Println("targetGID out of range")
				continue
			}
			if ck.Move(shard, targetGID) {
				fmt.Printf("Move shard %d -> GID %d submitted successfully\n", shard, targetGID)
			} else {
				fmt.Println("Move failed")
			}

		case "leave":
			if len(args) != 2 {
				fmt.Println("usage: leave <targetGID>")
				continue
			}
			targetGID, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Println("targetGID must be an integer")
				continue
			}
			if targetGID <= 0 || targetGID > Groups {
				fmt.Println("targetGID out of range")
				continue
			}
			if ck.Leave(targetGID) {
				fmt.Printf("Leave GID %d submitted successfully\n", targetGID)
			} else {
				fmt.Println("Leave failed")
			}

		case "join":
			if len(args) != 2 {
				fmt.Println("usage: join <GID>")
				continue
			}
			targetGID, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Println("GID must be an integer")
				continue
			}
			if targetGID <= 0 || targetGID > Groups {
				fmt.Println("GID out of range")
				continue
			}
			if ck.Join(targetGID) {
				fmt.Printf("Join GID %d submitted successfully\n", targetGID)
			} else {
				fmt.Println("Join failed")
			}

		default:
			fmt.Println("unknown command")
		}
	}
}
