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

const (
	OpQuery = "Query"
	OpJoin  = "Join"
	OpLeave = "Leave"
	OpMove  = "Move"
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
	lastCmd      map[int64]OpResult
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
		lastCmd:      make(map[int64]OpResult),
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

func (sc *ShardCtrler) submit(op Op) (OpResult, Err) {
	sc.mu.Lock()
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		return OpResult{}, ErrWrongLeader
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
		if res.ClientID != op.ClientID || res.Seq != op.Seq {
			return OpResult{}, ErrWrongLeader
		}
		return res, res.Err
	case <-time.After(500 * time.Millisecond):
		return OpResult{}, ErrTimeout
	}
}

func (sc *ShardCtrler) latestConfigLocked() Config {
	return copyConfig(sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) queryConfigLocked(num int) Config {
	if num == -1 || num >= len(sc.configs) {
		return copyConfig(sc.configs[len(sc.configs)-1])
	}
	return copyConfig(sc.configs[num])
}

func (sc *ShardCtrler) applyJoinLocked(gid int) Config {
	cfg := sc.latestConfigLocked()
	if _, exists := cfg.Groups[gid]; exists {
		return cfg
	}
	if servers, ok := groupConfig[gid]; ok {
		cfg.Groups[gid] = make(map[int]string, len(servers))
		for k, v := range servers {
			cfg.Groups[gid][k] = v
		}
	} else {
		cfg.Groups[gid] = map[int]string{}
	}
	cfg.Num++
	sc.rebalanceLocked(&cfg)
	sc.configs = append(sc.configs, cfg)
	return cfg
}

func (sc *ShardCtrler) applyLeaveLocked(gid int) Config {
	cfg := sc.latestConfigLocked()
	if _, exists := cfg.Groups[gid]; !exists {
		return cfg
	}
	delete(cfg.Groups, gid)
	cfg.Num++
	if len(cfg.Groups) == 0 {
		cfg.Shards = [NShards]int{}
		sc.configs = append(sc.configs, cfg)
		return cfg
	}
	sc.rebalanceLocked(&cfg)
	sc.configs = append(sc.configs, cfg)
	return cfg
}

func (sc *ShardCtrler) applyMoveLocked(shard, gid int) Config {
	cfg := sc.latestConfigLocked()
	if shard < 0 || shard >= NShards {
		return cfg
	}
	if gid != 0 {
		if _, ok := cfg.Groups[gid]; !ok {
			return cfg
		}
	}
	cfg.Shards[shard] = gid
	cfg.Num++
	sc.configs = append(sc.configs, cfg)
	return cfg
}

func (sc *ShardCtrler) rebalanceLocked(cfg *Config) {
	gids := make([]int, 0, len(cfg.Groups))
	for gid := range cfg.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	if len(gids) == 0 {
		cfg.Shards = [NShards]int{}
		return
	}
	target := make(map[int]int, len(gids))
	base := NShards / len(gids)
	rem := NShards % len(gids)
	for i, gid := range gids {
		target[gid] = base
		if i < rem {
			target[gid]++
		}
	}
	count := make(map[int]int, len(gids))
	free := make([]int, 0, NShards)
	for s, gid := range cfg.Shards {
		if gid == 0 {
			free = append(free, s)
			continue
		}
		if _, ok := cfg.Groups[gid]; !ok {
			cfg.Shards[s] = 0
			free = append(free, s)
			continue
		}
		count[gid]++
	}
	for _, gid := range gids {
		for count[gid] > target[gid] {
			shard := -1
			for s := NShards - 1; s >= 0; s-- {
				if cfg.Shards[s] == gid {
					shard = s
					break
				}
			}
			if shard < 0 {
				break
			}
			cfg.Shards[shard] = 0
			count[gid]--
			free = append(free, shard)
		}
	}
	sort.Ints(free)
	idx := 0
	for _, gid := range gids {
		for count[gid] < target[gid] {
			cfg.Shards[free[idx]] = gid
			count[gid]++
			idx++
		}
	}
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
		LastCmd map[int64]OpResult
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&data); err != nil {
		panic(fmt.Sprintf("ShardCtrler restore snapshot decode failed: %v", err))
	}
	sc.configs = data.Configs
	sc.lastSeq = data.LastSeq
	sc.lastCmd = data.LastCmd
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
	scCopyLastCmd := make(map[int64]OpResult)
	for cid, cmd := range sc.lastCmd {
		scCopyLastCmd[cid] = cmd
	}
	sc.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(struct {
		Configs []Config
		LastSeq map[int64]int
		LastCmd map[int64]OpResult
	}{
		Configs: scCopyConfigs,
		LastSeq: scCopyLastSeq,
		LastCmd: scCopyLastCmd,
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
		switch cmd := msg.Command.(type) {
		case raftapi.NoOp:
			sc.lastapplied = msg.CommandIndex
			index := msg.CommandIndex
			sc.mu.Unlock()
			sc.maybeTakeSnapshot(index)
			continue
		case Op:
			op := cmd
			var res OpResult
			last, exists := sc.lastSeq[op.ClientID]
			if exists && op.Seq <= last {
				res = sc.lastCmd[op.ClientID]
			} else {
				if op.Type == OpQuery {
					cfg := sc.queryConfigLocked(op.Num)
					res = OpResult{Err: OK, ClientID: op.ClientID, Seq: op.Seq, Config: cfg}
				} else {
					switch op.Type {
					case OpJoin:
						_ = sc.applyJoinLocked(op.GID)
					case OpLeave:
						_ = sc.applyLeaveLocked(op.GID)
					case OpMove:
						_ = sc.applyMoveLocked(op.Shard, op.GID)
					default:
						sc.mu.Unlock()
						panic(fmt.Sprintf("apply unknown optype: %s", op.Type))
					}
					res = OpResult{Err: OK, ClientID: op.ClientID, Seq: op.Seq}
				}
				sc.lastSeq[op.ClientID] = op.Seq
				sc.lastCmd[op.ClientID] = res
			}
			if ch, ok := sc.waitCh[msg.CommandIndex]; ok {
				select {
				case ch <- res:
				default:
				}
			}
			sc.lastapplied = msg.CommandIndex
			index := msg.CommandIndex
			sc.mu.Unlock()
			sc.maybeTakeSnapshot(index)
			continue
		default:
			sc.mu.Unlock()
			panic(fmt.Sprintf("unknown command type %T", msg.Command))
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) error {
	res, err := sc.submit(Op{Type: OpQuery, ClientID: args.ClientID, Seq: args.Seq, Num: args.Num})
	reply.Err = err
	if err == OK {
		reply.Config = res.Config
	}
	return nil
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) error {
	_, err := sc.submit(Op{Type: OpMove, ClientID: args.ClientID, Seq: args.Seq, Shard: args.Shard, GID: args.GID})
	reply.Err = err
	return nil
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) error {
	_, err := sc.submit(Op{Type: OpLeave, ClientID: args.ClientID, Seq: args.Seq, GID: args.GID})
	reply.Err = err
	return nil
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) error {
	_, err := sc.submit(Op{Type: OpJoin, ClientID: args.ClientID, Seq: args.Seq, GID: args.GID})
	reply.Err = err
	return nil
}
