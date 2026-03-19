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

// Query 调用 ShardCtrler.Query RPC
func (c *Client) Query(num int) shardctrler.Config {
	c.seq += 1
	args := shardctrler.QueryArgs{
		Num:      num,
		ClientID: c.clientID,
		Seq:      c.seq,
	}
	var reply shardctrler.QueryReply
	for {
		for _, addr := range c.peers {
			rpcClient, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				continue
			}
			err = rpcClient.Call("ShardCtrler.Query", &args, &reply)
			rpcClient.Close()
			if err == nil {
				switch reply.Err {
				case shardctrler.OK:
					return reply.Config
				case shardctrler.ErrWrongLeader:
					continue
				default:
					fmt.Printf("unexpected error from ShardCtrler: %v", reply.Err)
					continue
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
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
