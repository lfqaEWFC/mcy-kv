package shardkv

import (
	"bytes"
	"fmt"
	"mcy-kv/ctrlerclient"
	"mcy-kv/labgob"
	"mcy-kv/raftapi"
	"mcy-kv/shardctrler"
	"net/rpc"
	"sync"
	"time"
)

type Err string
type ShardState int

const (
	Serving ShardState = iota
	Pulling
	BePulling
	GC
	Inactived
)
const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrTimeout     Err = "ErrTimeout"
)

type Op struct {
	Type      string
	Key       string
	Value     string
	ClientID  int64
	Seq       int
	Config    shardctrler.Config
	Shard     int
	ConfigNum int
	Data      map[string]string
	ClientSeq map[int64]int64
	ClientCmd map[int64]OpResult
}

type OpResult struct {
	Err      Err
	Value    string
	ClientID int64
	Seq      int
}

type PutArgs struct {
	Key      string
	Value    string
	ClientID int64
	Seq      int
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientID int64
	Seq      int
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	Shard     int
	ConfigNum int
}

type PullShardReply struct {
	Err       Err
	Data      map[string]string
	ClientSeq map[int64]int64
	ClientCmd map[int64]OpResult
}

type DeleteShardArgs struct {
	Shard     int
	ConfigNum int
}

type DeleteShardReply struct {
	Err Err
}

type ShardServer struct {
	lastApplied   int
	maxraftstate  int
	mu            sync.Mutex
	applyCh       chan raftapi.ApplyMsg
	shardkv       map[int]map[string]string
	rf            raftapi.Raft
	waitCh        map[int]chan OpResult
	lastshardseq  map[int]map[int64]int
	lastshardcmd  map[int]map[int64]OpResult
	gid           int
	ck            *ctrlerclient.Client
	config        shardctrler.Config
	pendingConfig shardctrler.Config
	shardState    map[int]ShardState
}

func init() {
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
}

func call(addr string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return false
	}
	defer c.Close()

	done := make(chan error, 1)

	go func() {
		done <- c.Call(rpcname, args, reply)
	}()

	select {
	case err := <-done:
		return err == nil
	case <-time.After(500 * time.Millisecond):
		return false
	}
}
func NewServer(rf raftapi.Raft, applyCh chan raftapi.ApplyMsg, maxraftstate int, ck *ctrlerclient.Client, gid int) *ShardServer {
	shardkv := &ShardServer{
		rf:           rf,
		applyCh:      applyCh,
		shardkv:      make(map[int]map[string]string),
		lastshardseq: make(map[int]map[int64]int),
		waitCh:       make(map[int]chan OpResult),
		lastshardcmd: make(map[int]map[int64]OpResult),
		maxraftstate: maxraftstate,
		ck:           ck,
		gid:          gid,
		shardState:   make(map[int]ShardState, shardctrler.NShards),
	}
	for shard := 0; shard < shardctrler.NShards; shard++ {
		shardkv.shardState[shard] = Serving
		shardkv.shardkv[shard] = make(map[string]string)
		shardkv.lastshardseq[shard] = make(map[int64]int)
		shardkv.lastshardcmd[shard] = make(map[int64]OpResult)
	}
	go shardkv.applier()
	go shardkv.configPoller()
	go shardkv.Puller()
	go shardkv.gcWorker()
	return shardkv
}

func key_shard(key string) int {
	if len(key) == 0 {
		return 0
	}
	return int(key[0]) % shardctrler.NShards
}

func (shardkv *ShardServer) getWaitCh(index int) chan OpResult {
	ch, ok := shardkv.waitCh[index]
	if !ok {
		ch = make(chan OpResult, 1)
		shardkv.waitCh[index] = ch
	}
	return ch
}

func (shardkv *ShardServer) removeWaitCh(index int) {
	delete(shardkv.waitCh, index)
}

func (shardkv *ShardServer) applyConfig(newConfig shardctrler.Config) {
	oldConfig := shardkv.config
	for shard := 0; shard < shardctrler.NShards; shard++ {
		oldGid := oldConfig.Shards[shard]
		newGid := newConfig.Shards[shard]
		if newGid == shardkv.gid {
			if oldGid != shardkv.gid {
				if shardkv.shardState[shard] == Inactived {
					shardkv.shardState[shard] = Pulling
				}
			}
		} else {
			if oldGid == 0 {
				delete(shardkv.shardkv, shard)
				delete(shardkv.lastshardseq, shard)
				delete(shardkv.lastshardcmd, shard)
				if shardkv.shardState[shard] == Serving {
					shardkv.shardState[shard] = Inactived
				}
			}
			if oldGid == shardkv.gid {
				if shardkv.shardState[shard] == Serving {
					shardkv.shardState[shard] = BePulling
				}
			}
		}
	}
}

func (shardkv *ShardServer) allServing() bool {
	for _, state := range shardkv.shardState {
		if !(state == Serving || state == Inactived) {
			return false
		}
	}
	return true
}

func (shardkv *ShardServer) restoreFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	var data struct {
		Shardkv       map[int]map[string]string
		LastShardSeq  map[int]map[int64]int
		LastShardCmd  map[int]map[int64]OpResult
		Config        shardctrler.Config
		PendingConfig shardctrler.Config
		ShardState    map[int]ShardState
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&data); err != nil {
		panic(fmt.Sprintf("KVServer restoreFromSnapshot decode failed: %v", err))
	}
	shardkv.config = data.Config
	shardkv.shardkv = data.Shardkv
	shardkv.lastshardseq = data.LastShardSeq
	shardkv.lastshardcmd = data.LastShardCmd
	shardkv.pendingConfig = data.PendingConfig
	shardkv.shardState = data.ShardState
}

func (shardkv *ShardServer) maybeTakeSnapshot() {
	if shardkv.maxraftstate <= 0 {
		return
	}
	shardkv.mu.Lock()
	if shardkv.rf.PersistBytes() < shardkv.maxraftstate {
		shardkv.mu.Unlock()
		return
	}
	shardkvCopykv := make(map[int]map[string]string, len(shardkv.shardkv))
	for shard, kvMap := range shardkv.shardkv {
		newkv := make(map[string]string, len(kvMap))
		for k, v := range kvMap {
			newkv[k] = v
		}
		shardkvCopykv[shard] = newkv
	}
	shardkvCopyShardSeq := make(map[int]map[int64]int, len(shardkv.lastshardseq))
	for shard, clientMap := range shardkv.lastshardseq {
		shardkvCopyShardSeq[shard] = make(map[int64]int, len(clientMap))
		for cid, seq := range clientMap {
			shardkvCopyShardSeq[shard][cid] = seq
		}
	}
	shardkvCopyShardCmd := make(map[int]map[int64]OpResult, len(shardkv.lastshardcmd))
	for shard, clientMap := range shardkv.lastshardcmd {
		shardkvCopyShardCmd[shard] = make(map[int64]OpResult, len(clientMap))
		for cid, cmd := range clientMap {
			shardkvCopyShardCmd[shard][cid] = cmd
		}
	}
	shardkvCopyConfig := ctrlerclient.CopyConfig(shardkv.config)
	PendingCopyConfig := ctrlerclient.CopyConfig(shardkv.pendingConfig)
	ShardCopyState := make(map[int]ShardState, shardctrler.NShards)
	for cid, cmd := range shardkv.shardState {
		ShardCopyState[cid] = cmd
	}

	lastApplied := shardkv.lastApplied
	shardkv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(struct {
		Shardkv       map[int]map[string]string
		LastShardSeq  map[int]map[int64]int
		LastShardCmd  map[int]map[int64]OpResult
		Config        shardctrler.Config
		PendingConfig shardctrler.Config
		ShardState    map[int]ShardState
	}{
		Shardkv:       shardkvCopykv,
		LastShardSeq:  shardkvCopyShardSeq,
		LastShardCmd:  shardkvCopyShardCmd,
		Config:        shardkvCopyConfig,
		PendingConfig: PendingCopyConfig,
		ShardState:    ShardCopyState,
	}); err != nil {
		panic(fmt.Sprintf("KVServer maybeTakeSnapshot encode failed: %v", err))
	}
	snapshotBytes := w.Bytes()

	shardkv.rf.Snapshot(lastApplied, snapshotBytes)
	shardkv.mu.Lock()
	for idx := range shardkv.waitCh {
		if idx <= lastApplied {
			delete(shardkv.waitCh, idx)
		}
	}
	shardkv.mu.Unlock()
}

func (shardkv *ShardServer) applier() {
	for msg := range shardkv.applyCh {
		if msg.SnapshotValid {
			shardkv.mu.Lock()
			shardkv.restoreFromSnapshot(msg.Snapshot)
			shardkv.lastApplied = msg.SnapshotIndex
			shardkv.mu.Unlock()
			fmt.Printf("snapshot valid...\n")
			continue
		}
		if !msg.CommandValid {
			fmt.Printf("error command type...\n")
			continue
		}
		fmt.Printf("GID %d: APPILER PREPARES TO GET LOCK\n", shardkv.gid)
		shardkv.mu.Lock()
		fmt.Printf("GID %d: APPILER GET LOCK\n", shardkv.gid)
		switch op := msg.Command.(type) {
		case Op:
			if op.Type == "ConfigUpdate" {
				if op.Config.Num > shardkv.config.Num {
					if shardkv.pendingConfig.Num > shardkv.config.Num {
						break
					}
					shardkv.pendingConfig = ctrlerclient.CopyConfig(op.Config)
					shardkv.applyConfig(op.Config)
					fmt.Printf("pending config %d\n", op.Config.Num)
					if shardkv.allServing() {
						fmt.Printf("gid : %d\n", shardkv.gid)
						shardkv.config = ctrlerclient.CopyConfig(shardkv.pendingConfig)
						fmt.Println(shardkv.config)
					}
				} else {
					fmt.Printf("gid %d ", shardkv.gid)
					fmt.Printf("received old/duplicate config %d\n", op.Config.Num)
				}
			} else if op.Type == "InsertShard" {
				if op.ConfigNum != shardkv.pendingConfig.Num {
					break
				}
				shard := op.Shard
				if shardkv.shardState[shard] == Pulling {
					newKV := make(map[string]string)
					for k, v := range op.Data {
						newKV[k] = v
					}
					shardkv.shardkv[shard] = newKV
					newSeq := make(map[int64]int)
					for cid, seq := range op.ClientSeq {
						newSeq[cid] = int(seq)
					}
					shardkv.lastshardseq[shard] = newSeq
					newCmd := make(map[int64]OpResult)
					for cid, cmd := range op.ClientCmd {
						newCmd[cid] = cmd
					}
					shardkv.lastshardcmd[shard] = newCmd
					shardkv.shardState[shard] = GC

					fmt.Printf("gid %d shard %d: insert end\n", shardkv.gid, shard)
					fmt.Printf("gid %d:	", shardkv.gid)
					fmt.Println(shardkv.shardState)
				}
			} else if op.Type == "GCComplete" {
				if op.ConfigNum != shardkv.pendingConfig.Num {
					break
				}
				shard := op.Shard
				if shardkv.shardState[shard] == GC {
					shardkv.shardState[shard] = Serving
					fmt.Printf("gid %d shard %d: gc end\n", shardkv.gid, shard)
					fmt.Printf("gid %d:	", shardkv.gid)
					fmt.Println(shardkv.shardState)
					if shardkv.allServing() {
						shardkv.config = ctrlerclient.CopyConfig(shardkv.pendingConfig)
						fmt.Printf("gid : %d\n", shardkv.gid)
						fmt.Println(shardkv.config)
					}
				}
			} else if op.Type == "DeleteShard" {
				if op.ConfigNum != shardkv.pendingConfig.Num {
					break
				}
				shard := op.Shard
				if shardkv.shardState[shard] == BePulling {
					delete(shardkv.shardkv, shard)
					delete(shardkv.lastshardseq, shard)
					delete(shardkv.lastshardcmd, shard)
					shardkv.shardState[shard] = Inactived
					fmt.Printf("gid %d shard %d: inactive end\n", shardkv.gid, shard)
					fmt.Printf("gid %d:	", shardkv.gid)
					fmt.Println(shardkv.shardState)
					if shardkv.allServing() {
						shardkv.config = ctrlerclient.CopyConfig(shardkv.pendingConfig)
						fmt.Printf("gid : %d\n", shardkv.gid)
						fmt.Println(shardkv.config)
					}
					if ch, ok := shardkv.waitCh[msg.CommandIndex]; ok {
						select {
						case ch <- OpResult{}:
						default:
						}
					}
				}
			} else {
				shard := key_shard(op.Key)
				last := shardkv.lastshardseq[shard][op.ClientID]
				var res OpResult
				if op.Seq > last {
					switch op.Type {
					case "Put":
						if shardkv.config.Shards[shard] != shardkv.gid || shardkv.shardState[shard] != Serving {
							res = OpResult{Err: ErrWrongGroup, ClientID: op.ClientID, Seq: op.Seq}
							break
						}
						if shardkv.shardkv[shard] == nil {
							shardkv.shardkv[shard] = make(map[string]string)
						}
						shardkv.shardkv[shard][op.Key] = op.Value
						shardkv.lastshardseq[shard][op.ClientID] = op.Seq
						res = OpResult{Err: OK, ClientID: op.ClientID, Seq: op.Seq}

					case "Get":
						if shardkv.config.Shards[shard] != shardkv.gid || shardkv.shardState[shard] != Serving {
							res = OpResult{Err: ErrWrongGroup, ClientID: op.ClientID, Seq: op.Seq}
							break
						}
						value := shardkv.shardkv[shard][op.Key]
						res = OpResult{
							Err:      OK,
							Value:    value,
							ClientID: op.ClientID,
							Seq:      op.Seq,
						}
						shardkv.lastshardcmd[shard][op.ClientID] = res
						shardkv.lastshardseq[shard][op.ClientID] = op.Seq

					default:
						panic("KVServer: unknown operation type")
					}
				} else {
					if op.Type == "Get" {
						res = shardkv.lastshardcmd[shard][op.ClientID]
					} else {
						res = OpResult{Err: OK, ClientID: op.ClientID, Seq: op.Seq}
					}
				}
				if ch, ok := shardkv.waitCh[msg.CommandIndex]; ok {
					select {
					case ch <- res:
					default:
					}
				}
			}
		case raftapi.NoOp:
		default:
			fmt.Printf("Unknown command type: %T\n", msg.Command)
			panic("KVServer: unknown command type\n")
		}
		shardkv.lastApplied = msg.CommandIndex
		shardkv.mu.Unlock()
		shardkv.maybeTakeSnapshot()
	}
}

func (shardkv *ShardServer) configPoller() {
	time.Sleep(3 * time.Second) //给kv从raft收敛状态预留时间
	for {
		_, isLeader := shardkv.rf.GetState()
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		shardkv.mu.Lock()
		currentNum := shardkv.config.Num
		allServing := shardkv.allServing()
		pendingNum := shardkv.pendingConfig.Num
		shardkv.mu.Unlock()

		if allServing && pendingNum == currentNum {
			newConfig := shardkv.ck.Query(currentNum + 1)
			if newConfig.Num == currentNum+1 {
				shardkv.rf.Start(Op{Type: "ConfigUpdate", Config: newConfig})
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (shardkv *ShardServer) Puller() {
	for {
		_, isLeader := shardkv.rf.GetState()
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		shardkv.mu.Lock()
		pendingNum := shardkv.pendingConfig.Num
		for shard, state := range shardkv.shardState {
			if state != Pulling {
				continue
			}
			oldGID := shardkv.config.Shards[shard]
			if oldGID == 0 {
				op := Op{
					Type:      "InsertShard",
					Shard:     shard,
					Data:      make(map[string]string),
					ClientSeq: make(map[int64]int64),
					ConfigNum: pendingNum,
				}
				shardkv.mu.Unlock()
				shardkv.rf.Start(op)
				shardkv.mu.Lock()
				continue
			}
			servers, ok := shardkv.config.Groups[oldGID]
			if !ok {
				continue
			}
			serversCopy := make([]string, len(servers))
			for cid, srv := range servers {
				serversCopy[cid] = srv
			}
			shardkv.mu.Unlock()
			var shardData map[string]string
			var clientSeq map[int64]int64
			var clientCmd map[int64]OpResult
			success := false
			for _, srv := range serversCopy {
				args := PullShardArgs{
					Shard:     shard,
					ConfigNum: pendingNum,
				}
				reply := PullShardReply{}
				if call(srv, "ShardServer.PullShard", args, &reply) && reply.Err == OK {
					shardData = reply.Data
					clientSeq = reply.ClientSeq
					clientCmd = reply.ClientCmd
					success = true
					break
				}
			}
			if !success {
				shardkv.mu.Lock()
				continue
			}
			op := Op{
				Type:      "InsertShard",
				Shard:     shard,
				Data:      shardData,
				ClientSeq: clientSeq,
				ClientCmd: clientCmd,
				ConfigNum: pendingNum,
			}
			shardkv.rf.Start(op)

			shardkv.mu.Lock()
		}
		shardkv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (shardkv *ShardServer) gcWorker() {
	for {
		_, isLeader := shardkv.rf.GetState()
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		shardkv.mu.Lock()
		pendingNum := shardkv.pendingConfig.Num

		for shard, state := range shardkv.shardState {
			if state != GC {
				continue
			}
			oldGID := shardkv.config.Shards[shard]
			if oldGID == 0 {
				op := Op{
					Type:      "GCComplete",
					Shard:     shard,
					ConfigNum: pendingNum,
				}
				shardkv.mu.Unlock()
				shardkv.rf.Start(op)
				shardkv.mu.Lock()
				continue
			}
			servers, ok := shardkv.config.Groups[oldGID]
			if !ok {
				continue
			}
			serversCopy := make([]string, len(servers))
			for cid, srv := range servers {
				serversCopy[cid] = srv
			}
			shardkv.mu.Unlock()
			success := false
			for _, srv := range serversCopy {
				args := DeleteShardArgs{
					Shard:     shard,
					ConfigNum: pendingNum,
				}
				reply := DeleteShardReply{}

				if call(srv, "ShardServer.DeleteShard", args, &reply) && reply.Err == OK {
					success = true
					fmt.Printf("gid %d shard %d: ", shardkv.gid, shard)
					fmt.Printf("call delete success,next is gc\n")
					break
				}
			}
			if success {
				op := Op{
					Type:      "GCComplete",
					Shard:     shard,
					ConfigNum: pendingNum,
				}
				shardkv.rf.Start(op)
			}
			shardkv.mu.Lock()
		}
		shardkv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (shardkv *ShardServer) Put(args PutArgs, reply *PutReply) error {
	if args.Key == "" {
		reply.Err = ErrWrongGroup
		return nil
	}
	shard := key_shard(args.Key)
	shardkv.mu.Lock()
	if shardkv.config.Shards[shard] != shardkv.gid {
		shardkv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}
	if shardkv.lastshardseq[shard][args.ClientID] >= args.Seq {
		if shardkv.shardState[shard] != Serving {
			shardkv.mu.Unlock()
			reply.Err = ErrWrongGroup
			return nil
		}
		reply.Err = OK
		shardkv.mu.Unlock()
		return nil
	}
	if shardkv.shardState[shard] != Serving {
		shardkv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}
	shardkv.mu.Unlock()

	op := Op{
		Type:     "Put",
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}

	index, _, isLeader := shardkv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	shardkv.mu.Lock()
	ch := shardkv.getWaitCh(index)
	shardkv.mu.Unlock()

	defer func() {
		shardkv.mu.Lock()
		shardkv.removeWaitCh(index)
		shardkv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		if res.ClientID != op.ClientID || res.Seq != op.Seq {
			reply.Err = ErrWrongLeader
			return nil
		}
		reply.Err = res.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	return nil
}

func (shardkv *ShardServer) Get(args GetArgs, reply *GetReply) error {
	if args.Key == "" {
		reply.Err = ErrWrongGroup
		return nil
	}
	shard := key_shard(args.Key)
	shardkv.mu.Lock()
	if shardkv.config.Shards[shard] != shardkv.gid {
		shardkv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}
	if shardkv.lastshardseq[shard][args.ClientID] >= args.Seq {
		if shardkv.shardState[shard] != Serving {
			shardkv.mu.Unlock()
			reply.Err = ErrWrongGroup
			return nil
		}
		reply.Err = OK
		reply.Value = shardkv.lastshardcmd[shard][args.ClientID].Value
		shardkv.mu.Unlock()
		return nil
	}
	if shardkv.shardState[shard] != Serving {
		shardkv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}
	shardkv.mu.Unlock()

	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}

	index, _, isLeader := shardkv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	shardkv.mu.Lock()
	ch := shardkv.getWaitCh(index)
	shardkv.mu.Unlock()

	defer func() {
		shardkv.mu.Lock()
		shardkv.removeWaitCh(index)
		shardkv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		if res.ClientID != op.ClientID || res.Seq != op.Seq {
			reply.Err = ErrWrongLeader
			return nil
		}
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	return nil
}

func (shardkv *ShardServer) PullShard(args PullShardArgs, reply *PullShardReply) error {
	shardkv.mu.Lock()
	defer shardkv.mu.Unlock()
	_, isLeader := shardkv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}
	if args.ConfigNum != shardkv.pendingConfig.Num {
		reply.Err = ErrWrongGroup
		return nil
	}
	if args.Shard < 0 || args.Shard >= shardctrler.NShards {
		reply.Err = ErrWrongGroup
		return nil
	}
	if shardkv.shardState[args.Shard] != BePulling {
		reply.Err = ErrWrongGroup
		return nil
	}
	data := make(map[string]string)
	for k, v := range shardkv.shardkv[args.Shard] {
		data[k] = v
	}
	clientSeq := make(map[int64]int64)
	for cid, seq := range shardkv.lastshardseq[args.Shard] {
		clientSeq[cid] = int64(seq)
	}
	clientCmd := make(map[int64]OpResult)
	for cid, cmd := range shardkv.lastshardcmd[args.Shard] {
		clientCmd[cid] = cmd
	}
	reply.Err = OK
	reply.Data = data
	reply.ClientSeq = clientSeq
	reply.ClientCmd = clientCmd
	return nil
}

func (shardkv *ShardServer) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) error {
	shardkv.mu.Lock()
	_, isLeader := shardkv.rf.GetState()
	if !isLeader {
		shardkv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return nil
	}
	if args.ConfigNum != shardkv.pendingConfig.Num {
		shardkv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}
	shard := args.Shard
	if shardkv.shardState[shard] != BePulling {
		shardkv.mu.Unlock()
		reply.Err = OK
		return nil
	}
	op := Op{
		Type:      "DeleteShard",
		Shard:     shard,
		ConfigNum: args.ConfigNum,
	}
	shardkv.mu.Unlock()

	index, _, isLeader := shardkv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}
	shardkv.mu.Lock()
	ch := shardkv.getWaitCh(index)
	shardkv.mu.Unlock()
	defer func() {
		shardkv.mu.Lock()
		shardkv.removeWaitCh(index)
		shardkv.mu.Unlock()
	}()
	select {
	case res := <-ch:
		if res.ClientID != 0 || res.Seq != 0 {
			reply.Err = ErrWrongLeader
			return nil
		}
		reply.Err = OK

	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	return nil
}
