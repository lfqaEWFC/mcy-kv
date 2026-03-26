package shardkvclient

import (
	"fmt"
	"math/rand"
	"mcy-kv/ctrlerclient"
	"mcy-kv/shardctrler"
	"mcy-kv/shardkv"
	"net/rpc"
	"time"
)

type Clerk struct {
	leader   int
	clientID int64
	seq      int
	ctrlck   *ctrlerclient.Client
	config   shardctrler.Config
}

func nrand() int64 {
	return rand.Int63()
}

func key_shard(key string) int {
	if len(key) == 0 {
		return 0
	}
	return int(key[0]) % shardctrler.NShards
}

func MakeClerk(ctrlck *ctrlerclient.Client) *Clerk {
	config := ctrlck.Query(-1)
	ck := &Clerk{
		leader:   0,
		clientID: nrand(),
		seq:      0,
		ctrlck:   ctrlck,
		config:   config,
	}
	return ck
}

func call(addr string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return false
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	return err == nil
}

func (ck *Clerk) Put(key string, value string) {
	ck.seq++
	args := shardkv.PutArgs{
		Key:      key,
		Value:    value,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	for {
		shard := key_shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		if !ok || len(servers) == 0 {
			ck.config = ck.ctrlck.Query(-1)
			fmt.Println(ck.config)
			ck.leader = 0
			continue
		}
		for i := 0; i < len(servers); i++ {
			server := servers[ck.leader%len(servers)]
			var reply shardkv.PutReply
			ok := call(server, "ShardServer.Put", &args, &reply)
			if ok {
				fmt.Printf("Err is %s\n", reply.Err)
				switch reply.Err {
				case shardkv.OK:
					fmt.Printf("success: %s = %s\n", key, value)
					return

				case shardkv.ErrWrongLeader:

				case shardkv.ErrWrongGroup:
					ck.config = ck.ctrlck.Query(-1)
					fmt.Println(ck.config)
					ck.leader = 0
					goto RETRY

				case shardkv.ErrTimeout:
					time.Sleep(100 * time.Millisecond)
					goto RETRY
				}
			} else {
				fmt.Printf("rpc fail\n")
			}
			ck.leader = (ck.leader + 1) % len(servers)
		}
	RETRY:
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Get(key string) string {
	ck.seq++
	args := shardkv.GetArgs{
		Key:      key,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	for {
		shard := key_shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]
		if !ok || len(servers) == 0 {
			ck.config = ck.ctrlck.Query(-1)
			fmt.Println(ck.config)
			ck.leader = 0
			continue
		}
		for i := 0; i < len(servers); i++ {
			server := servers[ck.leader%len(servers)]
			var reply shardkv.GetReply
			ok := call(server, "ShardServer.Get", &args, &reply)
			if ok {
				switch reply.Err {
				case shardkv.OK:
					fmt.Printf("success: %s = %s\n", key, reply.Value)
					return reply.Value

				case shardkv.ErrWrongLeader:

				case shardkv.ErrWrongGroup:
					ck.config = ck.ctrlck.Query(-1)
					fmt.Println(ck.config)
					ck.leader = 0
					goto RETRY

				case shardkv.ErrTimeout:
					time.Sleep(100 * time.Millisecond)
					goto RETRY
				}
			} else {
				fmt.Printf("rpc fail\n")
			}
			ck.leader = (ck.leader + 1) % len(servers)
		}
	RETRY:
		time.Sleep(50 * time.Millisecond)
	}
}
