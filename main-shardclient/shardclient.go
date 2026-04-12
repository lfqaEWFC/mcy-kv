package main

/*import (
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
}*/

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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

	const concurrency = 10
	const duration = 30 * time.Second

	var totalOps int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// QPS 统计
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var lastOps int64
		for {
			select {
			case <-ticker.C:
				currOps := atomic.LoadInt64(&totalOps)
				fmt.Printf("QPS: %d\n", currOps-lastOps)
				lastOps = currOps
			case <-stopCh:
				return
			}
		}
	}()

	// 并发压测：每个 goroutine 一个 clerk
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			clientID := time.Now().UnixNano() + int64(id)
			ctrlerck := ctrlerclient.NewClient(ctrlerpeers, clientID)
			ck := shardkvclient.MakeClerk(ctrlerck)

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1000))

			for {
				select {
				case <-stopCh:
					return
				default:
					num := r.Intn(1_000_000)
					key := fmt.Sprintf("num%d", num)
					value := fmt.Sprintf("%d", num)

					ck.Put(value, key)
					atomic.AddInt64(&totalOps, 1)
				}
			}
		}(i)
	}

	time.Sleep(duration)
	close(stopCh)
	wg.Wait()

	fmt.Printf("Total operations: %d\n", totalOps)
}
