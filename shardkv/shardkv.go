package shardkv

import (
	"bytes"
	"fmt"
	"mcy-kv/ctrlerclient"
	"mcy-kv/labgob"
	"mcy-kv/raftapi"
	"mcy-kv/shardctrler"
	"sync"
	"time"
)

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrTimeout     Err = "ErrTimeout"
)

type Op struct {
	Type     string
	Key      string
	Value    string
	ClientID int64
	Seq      int
	Config   shardctrler.Config
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

type ShardServer struct {
	lastApplied   int
	maxraftstate  int
	mu            sync.Mutex
	applyCh       chan raftapi.ApplyMsg
	shardkv       map[int]map[string]string
	rf            raftapi.Raft
	waitCh        map[int]chan OpResult
	lastseq       map[int64]int
	lastcmd       map[int64]OpResult
	gid           int
	ck            *ctrlerclient.Client
	config        shardctrler.Config
	pendingConfig shardctrler.Config
}

func init() {
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
}

func NewServer(rf raftapi.Raft, applyCh chan raftapi.ApplyMsg, maxraftstate int, ck *ctrlerclient.Client, gid int) *ShardServer {
	shardkv := &ShardServer{
		rf:           rf,
		applyCh:      applyCh,
		shardkv:      make(map[int]map[string]string),
		lastseq:      make(map[int64]int),
		waitCh:       make(map[int]chan OpResult),
		lastcmd:      make(map[int64]OpResult),
		maxraftstate: maxraftstate,
		ck:           ck,
		gid:          gid,
	}

	go shardkv.applier()
	go shardkv.configPoller()

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

func (shardkv *ShardServer) restoreFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	var data struct {
		Shardkv       map[int]map[string]string
		LastSeq       map[int64]int
		LastCmd       map[int64]OpResult
		Config        shardctrler.Config
		PendingConfig shardctrler.Config
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&data); err != nil {
		panic(fmt.Sprintf("KVServer restoreFromSnapshot decode failed: %v", err))
	}
	shardkv.config = data.Config
	shardkv.shardkv = data.Shardkv
	shardkv.lastseq = data.LastSeq
	shardkv.lastcmd = data.LastCmd
	shardkv.pendingConfig = data.PendingConfig
}

func (shardkv *ShardServer) maybeTakeSnapshot() {
	if shardkv.maxraftstate <= 0 {
		return
	}
	shardkv.mu.Lock()
	fmt.Printf("PersisterBytes: %d\n", shardkv.rf.PersistBytes())
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
	shardkvCopySeq := make(map[int64]int, len(shardkv.lastseq))
	for cid, seq := range shardkv.lastseq {
		shardkvCopySeq[cid] = seq
	}
	shardkvCopyCmd := make(map[int64]OpResult, len(shardkv.lastcmd))
	for cid, cmd := range shardkv.lastcmd {
		shardkvCopyCmd[cid] = cmd
	}
	shardkvCopyConfig := ctrlerclient.CopyConfig(shardkv.config)
	PendingCopyConfig := ctrlerclient.CopyConfig(shardkv.pendingConfig)

	lastApplied := shardkv.lastApplied
	shardkv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(struct {
		Shardkv       map[int]map[string]string
		LastSeq       map[int64]int
		LastCmd       map[int64]OpResult
		Config        shardctrler.Config
		PendingConfig shardctrler.Config
	}{
		Shardkv:       shardkvCopykv,
		LastSeq:       shardkvCopySeq,
		LastCmd:       shardkvCopyCmd,
		Config:        shardkvCopyConfig,
		PendingConfig: PendingCopyConfig,
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
		fmt.Printf("index: %d\n", msg.CommandIndex)
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
		fmt.Printf("command valid...\n")
		shardkv.mu.Lock()
		switch op := msg.Command.(type) {
		case Op:
			if op.Type == "ConfigUpdate" {
				if op.Config.Num > shardkv.config.Num {
					shardkv.pendingConfig = ctrlerclient.CopyConfig(op.Config)
					//调试使用
					shardkv.config = ctrlerclient.CopyConfig(op.Config)
					shardkv.config.Num--
					//调试使用
					fmt.Printf("no pending config %d\n", op.Config.Num)
				} else {
					fmt.Printf("received old/duplicate config %d\n", op.Config.Num)
				}
			} else {
				applied := false
				wronggroup := false

				last := shardkv.lastseq[op.ClientID]
				if op.Seq > last {
					switch op.Type {
					case "Put":
						shard := key_shard(op.Key)
						if shardkv.config.Shards[shard] != shardkv.gid {
							wronggroup = true
							break
						}
						if shardkv.shardkv[shard] == nil {
							shardkv.shardkv[shard] = make(map[string]string)
						}
						shardkv.shardkv[shard][op.Key] = op.Value
						fmt.Printf("Put command: key=%s, value=%s, shard=%d\n", op.Key, op.Value, shard)
						applied = true
					case "Get":
						shard := key_shard(op.Key)
						if shardkv.config.Shards[shard] != shardkv.gid {
							wronggroup = true
							break
						}
						applied = true
						fmt.Printf("Get operation applied for key: %s\n", op.Key)
					default:
						panic("KVServer: unknown operation type")
					}

					if applied {
						shardkv.lastseq[op.ClientID] = op.Seq
					}
				}

				var res OpResult
				if wronggroup {
					res = OpResult{Err: ErrWrongGroup, ClientID: op.ClientID, Seq: op.Seq}
				} else {
					res = OpResult{Err: OK, ClientID: op.ClientID, Seq: op.Seq}
					if op.Type == "Get" {
						shard := key_shard(op.Key)
						if shardkv.shardkv[shard] != nil {
							res.Value = shardkv.shardkv[shard][op.Key]
						} else {
							res.Value = ""
						}
						shardkv.lastcmd[op.ClientID] = res
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
			fmt.Printf("No-op command applied\n")
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
	for {
		_, isLeader := shardkv.rf.GetState()
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		shardkv.mu.Lock()
		currentNum := shardkv.config.Num
		hasPending := shardkv.pendingConfig.Num > shardkv.config.Num
		shardkv.mu.Unlock()

		if !hasPending {
			newConfig := shardkv.ck.Query(currentNum + 1)
			if newConfig.Num == currentNum+1 {
				shardkv.rf.Start(Op{Type: "ConfigUpdate", Config: newConfig})
			}
		}
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
	if shardkv.lastseq[args.ClientID] >= args.Seq {
		reply.Err = OK
		shardkv.mu.Unlock()
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
	if shardkv.lastseq[args.ClientID] >= args.Seq {
		reply.Err = OK
		reply.Value = shardkv.lastcmd[args.ClientID].Value
		shardkv.mu.Unlock()
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
