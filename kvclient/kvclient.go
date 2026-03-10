package kvclient

import (
	"fmt"
	"math/rand"
	"mcy-kv/kv"
	"net/rpc"
	"time"
)

type Clerk struct {
	servers  map[int]string
	leader   int
	clientID int64
	seq      int
}

func nrand() int64 {
	return rand.Int63()
}

func MakeClerk(servers map[int]string) *Clerk {
	ck := &Clerk{
		servers:  servers,
		leader:   0,
		clientID: nrand(),
		seq:      0,
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
	args := kv.PutArgs{
		Key:      key,
		Value:    value,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	for {
		server := ck.servers[ck.leader]
		var reply kv.PutReply
		ok := call(server, "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case kv.OK:
				fmt.Printf("success: %s = %s\n", key, value)
				return
			case kv.ErrWrongLeader:
				fmt.Println("wrong leader")
			case kv.ErrTimeout:
				fmt.Println("timeout")
				time.Sleep(10 * time.Second)
				continue
			}
		} else {
			fmt.Println("rpc failed")
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Get(key string) string {
	ck.seq++
	args := kv.GetArgs{
		Key:      key,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	for {
		server := ck.servers[ck.leader]
		var reply kv.GetReply
		ok := call(server, "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case kv.OK:
				if reply.Value == "" {
					fmt.Printf("success: %s is not found\n", key)
					return reply.Value
				}
				fmt.Printf("success: %s = %s\n", key, reply.Value)
				return reply.Value
			case kv.ErrWrongLeader:
				fmt.Println("wrong leader")
			case kv.ErrTimeout:
				fmt.Println("timeout")
				time.Sleep(10 * time.Second)
				continue
			}
		} else {
			fmt.Println("rpc failed")
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}
