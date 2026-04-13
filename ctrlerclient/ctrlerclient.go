package ctrlerclient

import (
	"fmt"
	"mcy-kv/shardctrler"
	"net/rpc"
	"time"
)

// Client 封装 ShardKV 或其他节点访问 ShardCtrler 的 RPC
type Client struct {
	peers    map[int]string // ShardCtrler 节点地址列表
	clientID int64          // 唯一身份
	seq      int            // 自增请求序号
}

// NewClient 创建一个新的 ShardCtrler RPC 客户端
func NewClient(peers map[int]string, clientID int64) *Client {
	return &Client{
		peers:    peers,
		clientID: clientID,
		seq:      0,
	}
}

func CopyConfig(src shardctrler.Config) shardctrler.Config {
	var dst shardctrler.Config
	dst.Num = src.Num
	dst.Shards = src.Shards
	dst.Groups = make(map[int]map[int]string)
	for gid, innerMap := range src.Groups {
		newInner := make(map[int]string)
		for sid, addr := range innerMap {
			newInner[sid] = addr
		}
		dst.Groups[gid] = newInner
	}
	return dst
}

// Query 调用 ShardCtrler.Query RPC
func (c *Client) Query(num int) shardctrler.Config {
	c.seq += 1
	args := shardctrler.QueryArgs{
		Num:      num,
		ClientID: c.clientID,
		Seq:      c.seq,
	}
	var reply shardctrler.QueryReply
	id := 0
	for {
		addr := c.peers[id]
		rpcClient, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			id = (id + 1) % len(c.peers)
			continue
		}
		err = rpcClient.Call("ShardCtrler.Query", &args, &reply)
		rpcClient.Close()
		if err != nil {
			id = (id + 1) % len(c.peers)
			continue
		}
		switch reply.Err {
		case shardctrler.OK:
			return reply.Config
		case shardctrler.ErrWrongLeader:
			id = (id + 1) % len(c.peers)
		default:
			fmt.Printf("unexpected error : %v\n", reply.Err)
			return reply.Config
		}
	}
}

// 调用 Leave 删除 Config 中指定 gid 的 group
func (c *Client) Leave(targetGID int) bool {
	c.seq += 1
	args := shardctrler.LeaveArgs{
		GID:      targetGID,
		ClientID: c.clientID,
		Seq:      c.seq,
	}
	var reply shardctrler.LeaveReply
	id := 0
	for {
		addr := c.peers[id]
		rpcClient, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			id = (id + 1) % len(c.peers)
			continue
		}
		err = rpcClient.Call("ShardCtrler.Leave", &args, &reply)
		rpcClient.Close()
		if err != nil {
			id = (id + 1) % len(c.peers)
			continue
		}
		switch reply.Err {
		case shardctrler.OK:
			return true
		case shardctrler.ErrWrongLeader:
			id = (id + 1) % len(c.peers)
		case shardctrler.ErrTimeout:
			time.Sleep(100 * time.Millisecond)
		default:
			fmt.Printf("unexpected error: %v\n", reply.Err)
			return false
		}
	}
}

// 调用 Join 添加 Config 中指定 gid 的 group
func (c *Client) Join(targetGID int) bool {
	c.seq += 1
	args := shardctrler.JoinArgs{
		GID:      targetGID,
		ClientID: c.clientID,
		Seq:      c.seq,
	}
	var reply shardctrler.JoinReply
	id := 0
	for {
		addr := c.peers[id]
		rpcClient, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			id = (id + 1) % len(c.peers)
			continue
		}
		err = rpcClient.Call("ShardCtrler.Join", &args, &reply)
		rpcClient.Close()
		if err != nil {
			id = (id + 1) % len(c.peers)
			continue
		}
		switch reply.Err {
		case shardctrler.OK:
			return true
		case shardctrler.ErrWrongLeader:
			id = (id + 1) % len(c.peers)
		case shardctrler.ErrTimeout:
			time.Sleep(100 * time.Millisecond)
		default:
			fmt.Printf("unexpected error: %v\n", reply.Err)
			return false
		}
	}
}

// 调用 Heartbeat 发送心跳
func (c *Client) Heartbeat(gid int, configNum int, load float64) error {
	c.seq += 1
	args := shardctrler.HeartBeatArgs{
		GID:       gid,
		ClientID:  c.clientID,
		Seq:       c.seq,
		Num:       configNum,
		GroupLoad: load,
	}
	var reply shardctrler.HeartBeatReply
	id := 0
	for {
		addr := c.peers[id]
		rpcClient, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			id = (id + 1) % len(c.peers)
			continue
		}
		err = rpcClient.Call("ShardCtrler.Heartbeat", &args, &reply)
		rpcClient.Close()
		if err != nil {
			id = (id + 1) % len(c.peers)
			continue
		}
		switch reply.Err {
		case shardctrler.OK:
			return nil
		case shardctrler.ErrWrongLeader:
			id = (id + 1) % len(c.peers)
		default:
			fmt.Printf("unexpected error: %v\n", reply.Err)
			return nil
		}
	}
}
