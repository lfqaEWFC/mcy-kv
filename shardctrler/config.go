package shardctrler

import (
	"bytes"
	"fmt"
	"mcy-kv/labgob"
	"mcy-kv/raftapi"
	"sort"
	"sync"
	"time"
)

var groupConfig = map[int]map[int]string{
	1: {0: "127.0.0.1:8001", 1: "127.0.0.1:8004", 2: "127.0.0.1:8007", 3: "127.0.0.1:8010", 4: "127.0.0.1:8013"},
	2: {0: "127.0.0.1:8002", 1: "127.0.0.1:8005", 2: "127.0.0.1:8008", 3: "127.0.0.1:8011", 4: "127.0.0.1:8014"},
	3: {0: "127.0.0.1:8003", 1: "127.0.0.1:8006", 2: "127.0.0.1:8009", 3: "127.0.0.1:8012", 4: "127.0.0.1:8015"},
}

const NShards = 10
const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

type Err string
type Config struct {
	Num    int
	Shards [NShards]int
	Groups map[int]map[int]string
}
type Op struct {
	Type     string
	ClientID int64
	Seq      int
	Num      int
	Shard    int
	GID      int
}
type OpResult struct {
	Err      Err
	ClientID int64
	Seq      int
	Config   Config
}
type ShardCtrler struct {
	lastapplied  int
	maxraftstate int
	mu           sync.Mutex
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	configs      []Config
	lastSeq      map[int64]int
	waitCh       map[int]chan OpResult
}
type QueryArgs struct {
	Num      int
	ClientID int64
	Seq      int
}
type QueryReply struct {
	Err    Err
	Config Config
}
type MoveArgs struct {
	Shard    int
	GID      int
	ClientID int64
	Seq      int
}
type MoveReply struct {
	Err Err
}

type LeaveArgs struct {
	GID      int
	ClientID int64
	Seq      int
}

type LeaveReply struct {
	Err Err
}

type JoinArgs struct {
	GID      int
	ClientID int64
	Seq      int
}

type JoinReply struct {
	Err Err
}

func NewServer(rf raftapi.Raft, applyCh chan raftapi.ApplyMsg, maxraftstate int) *ShardCtrler {
	sc := &ShardCtrler{
		rf:           rf,
		applyCh:      applyCh,
		maxraftstate: maxraftstate,
		configs:      make([]Config, 1),
		lastSeq:      make(map[int64]int),
		waitCh:       make(map[int]chan OpResult),
	}
	labgob.Register(Op{})
	labgob.Register(Config{})

	sc.configs[0] = Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: map[int]map[int]string{},
	}
	cfg1 := Config{
		Num: 1,
		Shards: [NShards]int{
			1, 1, 1,
			2, 2, 2,
			3, 3, 3,
			1,
		},
		Groups: groupConfig,
	}

	sc.configs = append(sc.configs, cfg1)

	go sc.apply()

	return sc
}

func (sc *ShardCtrler) getWaitCh(index int) chan OpResult {
	ch, ok := sc.waitCh[index]
	if !ok {
		ch = make(chan OpResult, 1)
		sc.waitCh[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) removeWaitCh(index int) {
	delete(sc.waitCh, index)
}

func copyConfig(cfg Config) Config {
	newGroups := make(map[int]map[int]string)
	for gid, servers := range cfg.Groups {
		newServers := make(map[int]string)
		for k, v := range servers {
			newServers[k] = v
		}
		newGroups[gid] = newServers
	}
	return Config{
		Num:    cfg.Num,
		Shards: cfg.Shards,
		Groups: newGroups,
	}
}

func (sc *ShardCtrler) restoreFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	var data struct {
		Configs []Config
		LastSeq map[int64]int
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&data); err != nil {
		panic(fmt.Sprintf("ShardCtrler restore snapshot decode failed: %v", err))
	}
	sc.configs = data.Configs
	sc.lastSeq = data.LastSeq
}

func (sc *ShardCtrler) maybeTakeSnapshot(index int) {
	if sc.maxraftstate <= 0 {
		return
	}
	sc.mu.Lock()
	if sc.rf.PersistBytes() <= sc.maxraftstate {
		sc.mu.Unlock()
		return
	}
	lastApplied := index
	scCopyConfigs := make([]Config, len(sc.configs))
	for i, config := range sc.configs {
		scCopyConfigs[i] = copyConfig(config)
	}
	scCopyLastSeq := make(map[int64]int)
	for cid, seq := range sc.lastSeq {
		scCopyLastSeq[cid] = seq
	}
	sc.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(struct {
		Configs []Config
		LastSeq map[int64]int
	}{
		Configs: scCopyConfigs,
		LastSeq: scCopyLastSeq,
	})
	sc.rf.Snapshot(lastApplied, w.Bytes())
}

func (sc *ShardCtrler) apply() {
	for msg := range sc.applyCh {
		if msg.SnapshotValid {
			sc.mu.Lock()
			sc.restoreFromSnapshot(msg.Snapshot)
			sc.lastapplied = msg.SnapshotIndex
			sc.mu.Unlock()
			continue
		}

		if !msg.CommandValid {
			continue
		}
		sc.mu.Lock()
		switch op := msg.Command.(type) {
		case Op:
			last, ok := sc.lastSeq[op.ClientID]
			var cfg Config
			if op.Type == "Query" {
				if op.Num == -1 || op.Num >= len(sc.configs) {
					cfg = copyConfig(sc.configs[len(sc.configs)-1])
				} else {
					cfg = copyConfig(sc.configs[op.Num])
				}
			}
			if op.Type != "Query" && (!ok || op.Seq > last) {
				switch op.Type {
				case "Join":
					cfg = copyConfig(sc.configs[len(sc.configs)-1])
					if _, exists := cfg.Groups[op.GID]; !exists {
						if servers, ok := groupConfig[op.GID]; ok {
							cfg.Groups[op.GID] = make(map[int]string)
							for k, v := range servers {
								cfg.Groups[op.GID][k] = v
							}
						} else {
							cfg.Groups[op.GID] = map[int]string{}
						}
						allGIDs := make([]int, 0, len(cfg.Groups))
						for gid := range cfg.Groups {
							allGIDs = append(allGIDs, gid)
						}
						sort.Ints(allGIDs)
						for i := 0; i < NShards; i++ {
							cfg.Shards[i] = allGIDs[i%len(allGIDs)]
						}
						cfg.Num++
						sc.configs = append(sc.configs, cfg)
					} else {
						cfg = copyConfig(sc.configs[len(sc.configs)-1])
						fmt.Println(sc.configs[len(sc.configs)-1])
					}
					fmt.Println(sc.configs[len(sc.configs)-1])
				case "Leave":
					cfg = copyConfig(sc.configs[len(sc.configs)-1])
					if _, exists := cfg.Groups[op.GID]; exists {
						delete(cfg.Groups, op.GID)
						remainingGIDs := make([]int, 0, len(cfg.Groups))
						for gid := range cfg.Groups {
							remainingGIDs = append(remainingGIDs, gid)
						}
						sort.Ints(remainingGIDs)
						GroupEmptyFlag := false
						for i := 0; i < NShards; i++ {
							if cfg.Shards[i] == op.GID {
								if len(remainingGIDs) > 0 {
									cfg.Shards[i] =
										remainingGIDs[i%len(remainingGIDs)]
								} else {
									GroupEmptyFlag = true
									break
								}
							}
						}
						if !GroupEmptyFlag {
							cfg.Num++
							sc.configs = append(sc.configs, cfg)
						}
						fmt.Println(sc.configs[len(sc.configs)-1])
					} else {
						cfg = copyConfig(sc.configs[len(sc.configs)-1])
						fmt.Println(sc.configs[len(sc.configs)-1])
					}
				case "Move":
					if op.Shard < 0 || op.Shard >= NShards {
						panic(fmt.Sprintf("invalid shard number %d", op.Shard))
					}
					cfg = copyConfig(sc.configs[len(sc.configs)-1])
					if op.GID != 0 {
						if _, ok := cfg.Groups[op.GID]; !ok {
							fmt.Printf("Move: ")
							fmt.Printf("target GID %d not exists\n", op.GID)
							break
						}
					}
					cfg.Shards[op.Shard] = op.GID
					cfg.Num++
					sc.configs = append(sc.configs, cfg)
					fmt.Println(sc.configs[len(sc.configs)-1])
				default:
					panic(fmt.Sprintf("Apply unknown optype: %s", op.Type))
				}
				sc.lastSeq[op.ClientID] = op.Seq
			}
			res := OpResult{
				Err:      OK,
				ClientID: op.ClientID,
				Seq:      op.Seq,
				Config:   cfg,
			}
			if ch, ok := sc.waitCh[msg.CommandIndex]; ok {
				select {
				case ch <- res:
				default:
				}
			}
		case raftapi.NoOp:
			fmt.Printf("No-op command applied\n")
		default:
			panic(fmt.Sprintf("unknown command type %T", msg.Command))
		}
		sc.lastapplied = msg.CommandIndex
		index := msg.CommandIndex
		sc.mu.Unlock()
		sc.maybeTakeSnapshot(index)
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) error {
	op := Op{
		Type:     "Query",
		ClientID: args.ClientID,
		Seq:      args.Seq,
		Num:      args.Num,
	}
	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		reply.Err = ErrWrongLeader
		return nil
	}
	ch := sc.getWaitCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		sc.removeWaitCh(index)
		sc.mu.Unlock()
	}()
	select {
	case res := <-ch:
		reply.Err = res.Err
		reply.Config = res.Config
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	return nil
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) error {
	op := Op{
		Type:     "Move",
		ClientID: args.ClientID,
		Seq:      args.Seq,
		Shard:    args.Shard,
		GID:      args.GID,
	}
	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		reply.Err = ErrWrongLeader
		return nil
	}
	ch := sc.getWaitCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		sc.removeWaitCh(index)
		sc.mu.Unlock()
	}()
	select {
	case res := <-ch:
		reply.Err = res.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	return nil
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) error {
	op := Op{
		Type:     "Leave",
		ClientID: args.ClientID,
		Seq:      args.Seq,
		GID:      args.GID,
	}
	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		reply.Err = ErrWrongLeader
		return nil
	}
	ch := sc.getWaitCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		sc.removeWaitCh(index)
		sc.mu.Unlock()
	}()
	select {
	case res := <-ch:
		reply.Err = res.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	return nil
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) error {
	op := Op{
		Type:     "Join",
		ClientID: args.ClientID,
		Seq:      args.Seq,
		GID:      args.GID,
	}
	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		reply.Err = ErrWrongLeader
		return nil
	}
	ch := sc.getWaitCh(index)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		sc.removeWaitCh(index)
		sc.mu.Unlock()
	}()
	select {
	case res := <-ch:
		reply.Err = res.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	return nil
}
