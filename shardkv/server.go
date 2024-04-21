package shardkv

import (
	"../labrpc"
	"bytes"
	"log"
	"time"
)
import "../raft"
import "sync"
import "../labgob"
import "../shardmaster"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Debug = 1

	// -----------------------
	WORK              = 0
	MOVINGOUT         = 1
	WAITINGFORPUSHING = 2
	INITIAL           = 3
	NOTSERVING        = 4

	// ----------------------
	// applypushshards 返回值

	FUTUREPUSH  = 1
	OLDPUSH     = 2
	CORRECTPUSH = 0
	RETRY       = 3
	WRONGLEADER = 4
)

func Max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

func Min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

const (
	TIMEOUT = 500
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type             string
	Value            string
	Key              string
	ClientId         int64
	Sequence         int
	Term             int
	WrongGroup       bool
	ConfigInfo       shardmaster.Config
	PushData         map[string]string
	PushShards       []int
	SeqOnEachClient  map[int64]int
	RemainingPulling int
	PushingShardsGid int
}

type ShardKV struct {
	mu                   sync.Mutex
	me                   int
	rf                   *raft.Raft
	applyCh              chan raft.ApplyMsg
	make_end             func(string) *labrpc.ClientEnd
	gid                  int
	masters              []*labrpc.ClientEnd
	maxraftstate         int   // snapshot if log grows this big
	dead                 int32 // set by Kill()
	data                 map[string]string
	peers                []*labrpc.ClientEnd
	sequenceOnEachClient map[int64]int
	// sequence             int
	rpcChannel    map[int]chan Op // log index -> the channel of a RPC
	persister     *raft.Persister
	configClerk   *shardmaster.Clerk
	config        shardmaster.Config
	shardState    []int
	groupNeedPush map[int]bool
	// remainingPushingaaa  int
	remainingPull int
	// Your definitions here.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//--DPrintf("server(%d)get = %v\n", kv.me, args.Key)
	DPrintf("GET: server(%d) has received get = %v, shard = %v\n",
		kv.gid, args.Key, key2shard(args.Key))

	logEntry := Op{Type: "Get", Key: args.Key}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	sid := key2shard(args.Key)
	if kv.shardState[sid] != WORK {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(logEntry)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("GET: server(%d) is gonna insert get = %v, shard = %v\n",
		kv.gid, args.Key, key2shard(args.Key))
	kv.mu.Lock()
	kv.rpcChannel[index-1] = make(chan Op, 1)
	waitingChannel := kv.rpcChannel[index-1]
	kv.mu.Unlock()
	select {
	case logEntryReturnedFromRaft := <-waitingChannel:
		if logEntryReturnedFromRaft.WrongGroup {
			reply.Err = ErrWrongGroup
			DPrintf("GET: server(%d)get = %v, shard = %v,  wrong group\n", kv.gid, args.Key, key2shard(args.Key))
		} else if logEntryReturnedFromRaft.ClientId != logEntry.ClientId || logEntryReturnedFromRaft.Sequence != logEntry.Sequence {
			reply.Err = ErrWrongLeader
		} else {
			DPrintf("GET: server(%d)get = %v, shard = %v, success\n", kv.gid, args.Key, key2shard(args.Key))
			reply.Err = OK
			reply.Value = logEntryReturnedFromRaft.Value
		}
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrWrongLeader
		//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
	}
	kv.mu.Lock()
	delete(kv.rpcChannel, index-1)
	kv.mu.Unlock()

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// kv.rf.GetState()
	logEntry := Op{Type: args.Op, Value: args.Value, Key: args.Key, ClientId: args.ClientId, Sequence: args.Sequence}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	sid := key2shard(args.Key)
	if kv.shardState[sid] != WORK {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(logEntry)

	//--DPrintf("server(%d) = hello\n", kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// reply.NewLeader = -1
		return
	}
	DPrintf("PUTAPPEND: server(gid = %v) is going to  %v(%v, %v), shard = %v\n",
		kv.gid, args.Op, args.Key, args.Value, key2shard(args.Key))
	kv.mu.Lock()
	kv.rpcChannel[index-1] = make(chan Op, 1)
	waitingChannel := kv.rpcChannel[index-1]
	kv.mu.Unlock()

	select {
	case logEntryReturnedFromRaft := <-waitingChannel:

		if logEntryReturnedFromRaft.WrongGroup {
			reply.Err = ErrWrongGroup
			DPrintf("PUTAPPEND: server(gid = %v)'s %v(%v, %v), shard = %v, wrong group!\n", kv.gid, args.Op, args.Key, args.Value, key2shard(args.Key))
		} else if logEntryReturnedFromRaft.ClientId != logEntry.ClientId ||
			logEntryReturnedFromRaft.Sequence != logEntry.Sequence {
			reply.Err = ErrWrongLeader

		} else {
			DPrintf("PUTAPPEND: server(gid = %v)'s %v(%v, %v), shard = %v, success!\n", kv.gid, args.Op, args.Key, args.Value, key2shard(args.Key))
			reply.Err = OK
		}
	case <-time.After((TIMEOUT) * time.Millisecond):
		reply.Err = ErrWrongLeader

		//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
	}
	// close(waitingChannel)kv.config = entry.ConfigInfo
	// kv.mu.Unlock()
	//panic("")
	// //--DPrintf("The value of [0~4] is %v, %v, %v, %v, %v", kv.data["0"], kv.data["1"], kv.data["2"], kv.data["3"], kv.data["4"])
	kv.mu.Lock()
	delete(kv.rpcChannel, index-1)
	kv.mu.Unlock()
}

func (kv *ShardKV) PushShards(args *PushShardsArgs, reply *PushShardsReply) {
	// 这里我们复用op中的Term字段，因为它整个applypushshards中没有被用到!!!!!!!!!!!!!!!!!!!!!!!1
	logEntry := Op{
		Type:             "PushShards",
		Value:            "",
		Key:              "",
		ClientId:         -1,
		Sequence:         -1,
		Term:             -1,
		WrongGroup:       false,
		ConfigInfo:       shardmaster.Config{Num: args.ConfigNum},
		PushData:         args.Data,
		PushShards:       args.PushShards,
		SeqOnEachClient:  args.SeqOnEachClient,
		RemainingPulling: kv.remainingPull - 1,
	}

	index, _, isLeader := kv.rf.Start(logEntry)
	//--DPrintf("server(%d) = hello\n", kv.me)
	if !isLeader {
		reply.Success = false
		reply.ErrType = WRONGLEADER
		return
	}
	kv.mu.Lock()

	kv.rpcChannel[index-1] = make(chan Op, 1)
	waitingChannel := kv.rpcChannel[index-1]
	kv.mu.Unlock()
	// 这里我们复用op中的Term字段，因为它整个applypushshards中没有被用到!!!!!!!!!!!!
	// 这里复用Term字段用于表示pushshards的结果
	select {
	case logEntryReturnedFromRaft := <-waitingChannel:
		// 这里是为了确保返回的log和插入的log是同一条，
		// 因为这里没有clinetId和Sequence
		if logEntryReturnedFromRaft.ConfigInfo.Num != logEntry.ConfigInfo.Num {
			reply.Success = false
			DPrintf("PUSHSHARDS: get wrong log entry, gid = %v, confNum = %v, shards = %v, data = %v",
				kv.gid, logEntryReturnedFromRaft.ConfigInfo.Num, args.PushShards, args.Data)
			reply.ErrType = RETRY
			//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
		} else if logEntryReturnedFromRaft.Term != CORRECTPUSH {
			if logEntryReturnedFromRaft.Term == FUTUREPUSH {
				// 这里复用Term字段用于表示pushshards的结果
				DPrintf("PUSHSHARDS: get future pushing shards, gid = %v, confNum = %v, shards = %v, data = %v",
					kv.gid, logEntryReturnedFromRaft.ConfigInfo.Num, args.PushShards, args.Data)
				reply.ErrType = FUTUREPUSH
				reply.Success = false
			} else if logEntryReturnedFromRaft.Term == OLDPUSH {
				DPrintf("PUSHSHARDS: get old pushing shards, gid = %v, confNum = %v, shards = %v, data = %v",
					kv.gid, logEntryReturnedFromRaft.ConfigInfo.Num, args.PushShards, args.Data)
				reply.ErrType = OLDPUSH
				reply.Success = false
			} else {
				panic("hihi")
			}

			reply.Success = false
		} else {
			DPrintf("PUSHSHARDS: over, gid = %v, confNum = %v, shards = %v, data = %v",
				kv.gid, logEntryReturnedFromRaft.ConfigInfo.Num, args.PushShards, args.Data)
			reply.Success = true
		}
	case <-time.After((TIMEOUT) * time.Millisecond):
		reply.Success = false
		DPrintf("PUSHSHARDS: timeout!, gid = %v, confNum = %v, shards = %v, data = %v",
			kv.gid, logEntry.ConfigInfo.Num, args.PushShards, args.Data)
		reply.ErrType = RETRY

	}
	kv.mu.Lock()
	delete(kv.rpcChannel, index-1)
	kv.mu.Unlock()
}

func UpdategroupNeedPush(kv *ShardKV, logEntry Op) bool {
	index, _, isLeader := kv.rf.Start(logEntry)
	var returnValue bool
	//--DPrintf("server(%d) = hello\n", kv.me)
	if !isLeader {
		returnValue = false
		return false
	}
	kv.mu.Lock()

	kv.rpcChannel[index-1] = make(chan Op, 1)
	waitingChannel := kv.rpcChannel[index-1]
	kv.mu.Unlock()
	// 这里我们复用op中的Term字段，因为它整个applypushshards中没有被用到!!!!!!!!!!!!1
	select {
	case logEntryReturnedFromRaft := <-waitingChannel:
		if logEntryReturnedFromRaft.ConfigInfo.Num != logEntry.ConfigInfo.Num ||
			logEntryReturnedFromRaft.PushingShardsGid != logEntry.PushingShardsGid {
			returnValue = false
			//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)

		} else {
			DPrintf("UPDATEGROUPNEEDPUSH: over, gid = %v, confNum = %v",
				kv.gid, logEntryReturnedFromRaft.ConfigInfo.Num)
			returnValue = true
		}
	case <-time.After((TIMEOUT) * time.Millisecond):
		returnValue = false

	}
	kv.mu.Lock()
	delete(kv.rpcChannel, index-1)
	kv.mu.Unlock()
	return returnValue
}

// 不需要上锁
func ApplyPushShards(kv *ShardKV, logEntry Op) int {
	data := logEntry.PushData
	PushShards := logEntry.PushShards
	configNum := logEntry.ConfigInfo.Num
	seqOnAnotherGroup := logEntry.SeqOnEachClient
	// remainingPull := logEntry.RemainingPulling
	if configNum > kv.config.Num {
		DPrintf("APPLYSHARDS: future push! me = %v, data = %v, pushShards = %v", kv.gid, data, PushShards)
		return FUTUREPUSH
	}

	if configNum < kv.config.Num {
		return OLDPUSH
	}

	DPrintf("APPLYSHARDS: me = %v is going to be pushed, data = %v, pushShards = %v", kv.gid, data, PushShards)
	for k, v := range data {
		sid := key2shard(k)
		if kv.shardState[sid] == WAITINGFORPUSHING {
			kv.data[k] = v
		}
	}

	for _, sid := range PushShards {
		if kv.shardState[sid] == WAITINGFORPUSHING {
			kv.shardState[sid] = WORK
			kv.remainingPull--
		} else {
			DPrintf("APPLYSHARDS:me: %v, shard %v state = %v", kv.gid, sid, kv.shardState[sid])
		}

	}

	for cid, seq := range seqOnAnotherGroup {
		kv.sequenceOnEachClient[cid] = Max(kv.sequenceOnEachClient[cid], seq)
	}
	// kv.remainingPull = Min(kv.remainingPull, remainingPull)

	DPrintf(""+
		"APPLYSHARDS: Group %v PushShards over. data = %v, shardsvector = %v, shardsState = %v", kv.gid, data, PushShards, kv.shardState)
	return CORRECTPUSH
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

type SendingShardsToOtherGroupRpcReply struct {
	ok    bool
	reply PushShardsReply
}

func SendingShardsToOtherGroup(kv *ShardKV, gid int, data map[string]string,
	config shardmaster.Config, PushShards []int, seqOnEachClient map[int64]int) {
	serverNames := config.Groups[gid]
	args := &PushShardsArgs{
		ConfigNum:       config.Num,
		Data:            data,
		PushShards:      PushShards,
		SeqOnEachClient: seqOnEachClient,
	}
	// panic("")
	var reply PushShardsReply
	//over := false
	var ok bool
	//for !over {
	for _, serverName := range serverNames {
		time.Sleep(TIMEOUT * time.Millisecond)
		serverPort := kv.make_end(serverName)
		DPrintf("%v is sending PushShards to %v, args = %v", kv.gid, gid, args)

		//tempChan := make(chan SendingShardsToOtherGroupRpcReply)
		//go SendingShardsToOtherGroupRpc(serverPort, args, tempChan)
		//select {
		//case response := <-tempChan:
		//	ok = response.ok
		//	reply = response.reply
		//case <-time.After(TIMEOUT * 4 * time.Millisecond):
		//	ok = false
		//	DPrintf("SendingShardsToOtherGroup:Timeout\n")
		//}

		ok = serverPort.Call("ShardKV.PushShards", args, &reply)
		if ok {
			DPrintf("%v sent PushShards to %v and got reply, args = %v, reply = %v", kv.gid, gid, args, reply)
			if reply.Success || reply.ErrType == OLDPUSH {
				//over = true
				break
			}
		}

	}
	if reply.Success || reply.ErrType == OLDPUSH {
		//kv.mu.Lock()
		if config.Num == kv.config.Num {

			logEntry := Op{
				Type:             "OverPush",
				ConfigInfo:       shardmaster.Config{Num: args.ConfigNum},
				PushingShardsGid: gid,
			}
			DPrintf("%v sent PushShards to %v over and is gonna update groupneedpush, args = %v, groupNeedPush = %v", kv.gid, gid, args, kv.groupNeedPush)
			for !UpdategroupNeedPush(kv, logEntry) {

			}
			DPrintf("%v sent PushShards to %v totally over, args = %v, groupNeedPush = %v", kv.gid, gid, args, kv.groupNeedPush)
		} else {
			//kv.mu.Unlock()
		}
		DPrintf("%v sent PushShards to %v over but not sucess, args = %v, groupNeedPush = %v", kv.gid, gid, args, kv.groupNeedPush)
	}

	//}

	//} else if reply.ErrType == OLDPUSH {
	//	kv.mu.Lock()
	//	if config.Num == kv.config.Num {
	//		logEntry := Op{
	//			Type:             "OverPush",
	//			ConfigInfo:       shardmaster.Config{Num: args.ConfigNum},
	//			PushingShardsGid: gid,
	//		}
	//		kv.rf.Start(logEntry)
	//	}
	//	DPrintf("%v sent old PushShards to %v, args = %v, groupNeedPush = %v", kv.gid, gid, args, kv.groupNeedPush)
	//	kv.mu.Unlock()
	//} else {
	//	panic("hihihihi")
	//}

}

func SendingShardsToOtherGroupRpc(port *labrpc.ClientEnd, args *PushShardsArgs, tempChan chan SendingShardsToOtherGroupRpcReply) {
	var reply PushShardsReply
	var temp SendingShardsToOtherGroupRpcReply
	ok := port.Call("ShardKV.PushShards", args, &reply)
	temp.ok = ok
	temp.reply = reply
	tempChan <- temp
}

// ApplyReconfig
// 1. 先进行Push操作
// 2. 等待其他Group给自己Push
// 3. 更新Config

func ApplyReconfig(kv *ShardKV, entry Op) {
	// DPrintf("me1: %v", kv.gid)
	//_, isLeader := kv.rf.GetState()
	//// kv.mu.Lock()
	//if isLeader {
	//	DPrintf("group(%v) is going to applying config %v, i am leader", kv.gid, entry.ConfigInfo.Num)
	//} else {
	//	// DPrintf("group(%v) is going to applying config %v", kv.gid, entry.ConfigInfo.Num)
	//}

	if entry.ConfigInfo.Num != kv.config.Num+1 {
		// kv.mu.Unlock()
		return
	}
	// panic("this function need to initialize a clerk")

	newShards := make(map[int]int)
	currentShards := make(map[int]int)
	// data := make(map[int]map[string]string)
	kv.groupNeedPush = make(map[int]bool)

	//for idx, _ := range data {
	//	data[idx] = make(map[string]string)
	//}
	newShardsOnEachGroup := make(map[int][]int)

	// 找到新的Config里Shards和Gid的对应关系
	for shardId, Gid := range entry.ConfigInfo.Shards {
		newShards[shardId] = Gid
	}

	for shardId, Gid := range kv.config.Shards {
		currentShards[shardId] = Gid
	}

	// 寻找不再负责的shard
	for sid, gid := range currentShards {
		if gid != kv.gid {
			continue
		}

		if newShards[sid] != kv.gid {
			// panic("")
			kv.shardState[sid] = MOVINGOUT
			newGid := newShards[sid]
			kv.groupNeedPush[newGid] = true
			// newShardsOnEachGroup[newGid] = append(newShardsOnEachGroup[newGid], sid)
		}
	}

	// DPrintf("gid : %v, me = %v, newshard = %v", kv.gid, kv.me, newShardsOnEachGroup)
	if entry.ConfigInfo.Num > 1 {
		for sid, gid := range newShards {
			if gid != kv.gid {
				continue
			}

			if currentShards[sid] != kv.gid {
				kv.shardState[sid] = WAITINGFORPUSHING
				kv.remainingPull++
			}

		}
	} else {
		// panic("")
		for sid, gid := range newShards {
			if gid != kv.gid {
				continue
			}
			kv.shardState[sid] = WORK

		}
	}

	//// 把迁出的Shards Push出去
	//for k, v := range kv.data {
	//	shardId := key2shard(k)
	//
	//	// 对应不再负责的kv pair
	//	// gid := kv.gid
	//	if kv.shardState[shardId] == MOVINGOUT {
	//		// panic("")
	//
	//		newGid := newShards[shardId]
	//		if data[newGid] == nil {
	//			data[newGid] = make(map[string]string)
	//		}
	//
	//		data[newGid][k] = v
	//
	//	}
	//}
	//// DPrintf("data = %v", data)
	//shardStateCopy := make([]int, 10)
	//copy(shardStateCopy, kv.shardState)
	//
	kv.config = entry.ConfigInfo
	//// kv.mu.Unlock()
	//// panic("")
	//
	//// 非Leader不负责发RPC！
	//if _, isLeader := kv.rf.GetState(); !isLeader {
	//	return
	//}
	//
	//for gid, sidVector := range newShardsOnEachGroup {
	//	if len(sidVector) == 0 || gid == kv.gid {
	//		continue
	//	}
	//	//panic("")
	//	go SendingShardsToOtherGroup(kv, gid, data[gid], entry.ConfigInfo, newShardsOnEachGroup[gid], kv.sequenceOnEachClient)
	//}

	DPrintf("Group %v has applied config %v\n\tShardState: %v, config = %v, newShardsOnEachGroup = %v, groupNeedPush = %v", kv.gid, kv.config.Num, kv.shardState, kv.config, newShardsOnEachGroup, kv.groupNeedPush)
}

func DaemonOfApplying(kv *ShardKV) {
	for entry := range kv.applyCh {
		// time.Sleep(TIMEOUT / 10 * time.Millisecond)
		kv.mu.Lock()

		//kv.currentApplyMsg = entry
		if !entry.CommandValid {
			// panic("")
			r := bytes.NewBuffer(entry.Snapshot)
			d := labgob.NewDecoder(r)
			err := d.Decode(&kv.data)
			if err != nil {
				panic("Server Decode Error1!" + err.Error())
			}
			//--DPrintf("data:%v", kv.data)

			err = d.Decode(&kv.sequenceOnEachClient)
			if err != nil {
				panic("Server Decode Error2!" + err.Error())
			}

			tempConfig := shardmaster.Config{}
			err = d.Decode(&tempConfig)
			if err != nil {
				panic("Server Decode Error3!" + err.Error())
			}
			kv.config = tempConfig

			err = d.Decode(&kv.shardState)
			if err != nil {
				panic("Server Decode Error4!" + err.Error())
			}

			DPrintf("before snapshot, groupNeedPush = %v", kv.groupNeedPush)
			err = d.Decode(&kv.groupNeedPush)
			if err != nil {
				panic("Server Decode Error!" + err.Error())
			}
			DPrintf("after snapshot, groupNeedPush = %v", kv.groupNeedPush)

			err = d.Decode(&kv.remainingPull)
			if err != nil {
				panic("Server Decode Error5!" + err.Error())
			}

			//virtualLogEntry := Op{
			//	ConfigInfo: tempConfig,
			//}
			//kv.config.Num = tempConfig.Num - 1

			// SendShardsWhenRestarting(kv, tempConfig)

			// panic("")
			kv.mu.Unlock()
			continue
		}

		tempEntry := entry.Command.(Op)
		// tempConfig := kv.configClerk.Query(-1)
		shardResponsibleForCurrentKey := key2shard(entry.Command.(Op).Key)

		// kv.config = tempConfig
		if entry.Command.(Op).Type == "Put" ||
			entry.Command.(Op).Type == "Append" {
			// panic("")
			if kv.shardState[shardResponsibleForCurrentKey] != WORK {
				DPrintf("wrong group %v! gid: %v, key: %v", tempEntry.Type, kv.gid, tempEntry.Key)
				tempEntry.WrongGroup = true
				// panic("")
			} else {
				tempEntry.WrongGroup = false

				if !tempEntry.WrongGroup && kv.sequenceOnEachClient[entry.Command.(Op).ClientId] < entry.Command.(Op).Sequence {
					switch tempEntry.Type {
					case "Put":
						kv.data[entry.Command.(Op).Key] = entry.Command.(Op).Value
					case "Append":
						//panic(kv.data[entry.Command.(Op).Key] + entry.Command.(Op).Value)
						kv.data[entry.Command.(Op).Key] = kv.data[entry.Command.(Op).Key] + entry.Command.(Op).Value
					default:
						panic("KVSERVER FUNC(Excute): WRONG OPERATION")
					}
					kv.sequenceOnEachClient[entry.Command.(Op).ClientId] = raft.Max(kv.sequenceOnEachClient[entry.Command.(Op).ClientId], entry.Command.(Op).Sequence)
				}
			}
		} else if entry.Command.(Op).Type == "Get" {
			if kv.shardState[shardResponsibleForCurrentKey] != WORK {
				DPrintf("wrong group Get! gid: %v, key: %v, configNum = %v, ShardState = %v", kv.gid, tempEntry.Key, kv.config.Num, kv.shardState)
				tempEntry.WrongGroup = true
				// panic("")
			} else {
				tempEntry.WrongGroup = false
				tempEntry.Value = kv.data[tempEntry.Key]
			}

		} else if entry.Command.(Op).Type == "Reconfig" {
			ApplyReconfig(kv, entry.Command.(Op))
		} else if entry.Command.(Op).Type == "PushShards" {
			ok := ApplyPushShards(kv, entry.Command.(Op))
			tempEntry.Term = ok
			// 这里我们复用op中的Term字段，因为它整个applypushshards中没有被用到
		} else if entry.Command.(Op).Type == "OverPush" {

			// ApplyOverPush(kv, entry.Command.(Op).PushingShardsGid)
			gid := entry.Command.(Op).PushingShardsGid
			shouldClean := false
			if entry.Command.(Op).ConfigInfo.Num == kv.config.Num {
				for sid, gidTemp := range kv.config.Shards {
					if kv.shardState[sid] == MOVINGOUT && gidTemp == gid {
						kv.shardState[sid] = NOTSERVING
						shouldClean = true
					}
				}
				kv.groupNeedPush[gid] = false
			}

			if shouldClean {
				for key, _ := range kv.data {
					sid := key2shard(key)
					if kv.shardState[sid] == NOTSERVING {
						delete(kv.data, key)
					}
				}
			}

		} else {
			panic("Unknown op type")
		}

		waitingChannelOnRpc, ok := kv.rpcChannel[entry.CommandIndex-1]

		if ok {
			select {
			case waitingChannelOnRpc <- tempEntry:
				// case <-time.After((TIMEOUT / 10) * time.Millisecond):
			}
			// kick their ass
			delete(kv.rpcChannel, entry.CommandIndex-1)
		}

		kv.mu.Unlock()
		SnapShot(kv, entry.CommandIndex-1)
	}

}

func SendShardsWhenRestarting(kv *ShardKV, config shardmaster.Config) {
	_, isLeader := kv.rf.GetState()
	if isLeader {
		DPrintf("group(%v) is going to applying config %v from restart!", kv.gid, config.Num)
	}

	newShards := make(map[int]int)
	currentShards := make(map[int]int)
	data := make(map[int]map[string]string)
	newShardsOnEachGroup := make(map[int][]int)

	// 找到新的Config里Shards和Gid的对应关系
	for shardId, Gid := range config.Shards {
		newShards[shardId] = Gid
	}

	for shardId, Gid := range kv.config.Shards {
		currentShards[shardId] = Gid
	}

	// 寻找不再负责的shard
	for sid, gid := range currentShards {
		if gid != kv.gid {
			continue
		}
		if newShards[sid] != gid {
			// kv.shardState[sid] = MOVINGOUT
			newGid := newShards[sid]
			// kv.groupNeedPush[newGid] = true
			newShardsOnEachGroup[newGid] = append(newShardsOnEachGroup[newGid], sid)
		}
	}

	// 把迁出的Shards Push出去
	for k, v := range kv.data {
		shardId := key2shard(k)

		// 对应不再负责的kv pair
		if kv.shardState[shardId] == MOVINGOUT {
			// panic("")

			newGid := newShards[shardId]
			if data[newGid] == nil {
				data[newGid] = make(map[string]string)
			}

			data[newGid][k] = v

		}
	}

	for gid, sidVector := range newShardsOnEachGroup {
		if len(sidVector) == 0 || gid == kv.gid {
			continue
		}
		go SendingShardsToOtherGroup(kv, gid, data[gid], config, newShardsOnEachGroup[gid], kv.sequenceOnEachClient)
	}

	// DPrintf("Group %v has applied config %v\n\tShardState: %v, config = %v, newShardsOnEachGroup = %v from restart", kv.gid, kv.config.Num, kv.shardState, kv.config, newShardsOnEachGroup)

}

// 拉取新的Config的线程,只有Leader才能正常工作
// 向Master拉取Config
func DaemonOfReconfig(kv *ShardKV) {
	for {
		// 只有Group Leader才允许Reconfigure!!!!
		time.Sleep(TIMEOUT * time.Millisecond)

		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		} else {
			DPrintf("DAEMONOFRECONFIG: my gid = %v, my raftpeer = %v, term = %v", kv.gid, kv.rf.Me, kv.rf.CurrentTerm)
		}
		shouldPullNewConfig := true
		kv.mu.Lock()
		DPrintf("DAEMONOFRECONFIG: Group %v is gonna fetch config %v", kv.gid, kv.config.Num+1)
		// DPrintf("Group %v fetch config2222", kv.gid)

		if kv.remainingPull != 0 {
			DPrintf("DAEMONOFRECONFIG: Group %v is gonna fetch config %v but hasn't finished last config of pulling"+
				"\n remainingpulling = %v, remainingPushing = %v, shardstate = %v, raftpeer = %v", kv.gid,
				kv.config.Num+1, kv.remainingPull, kv.groupNeedPush, kv.shardState, kv.rf.Me)

			shouldPullNewConfig = false
			//kv.mu.Unlock()
			//continue
		}

		// noGroupNeedToPush := true
		for _, haveNotPushed := range kv.groupNeedPush {
			if haveNotPushed {
				// noGroupNeedToPush = false
				shouldPullNewConfig = false
				DPrintf("DAEMONOFRECONFIG: Group %v is gonna fetch config %v but hasn't finished last config of pushing"+
					"\n remainingpulling = %v, remainingPushing = %v, shardstate = %v, raftpeer = %v", kv.gid, kv.config.Num+1,
					kv.remainingPull, kv.groupNeedPush, kv.shardState, kv.rf.Me)
				break
			}
		}

		//if !noGroupNeedToPush {
		//	DPrintf("DAEMONOFRECONFIG: Group %v is gonna fetch config %v but hasn't finished last config of pushing"+
		//		"\n remainingpulling = %v, remainingPushing = %v, shardstate = %v", kv.gid, kv.config.Num+1,
		//		kv.remainingPull, kv.groupNeedPush, kv.shardState)
		//	kv.mu.Unlock()
		//	continue
		//}

		// DPrintf("Group %v fetch config3333", kv.gid)
		// 向ShardMaster查询

		// DPrintf("Group %v fetch config", kv.gid)
		if shouldPullNewConfig {
			temp := kv.configClerk.Query(kv.config.Num + 1)
			if temp.Num != kv.config.Num+1 {
				DPrintf("Group %v wanted to fetch config %v, but got %v", kv.gid, kv.config.Num+1, temp.Num)
				kv.mu.Unlock()
				continue
			}

			DPrintf("Group %v has fetched config %v", kv.gid, temp.Num)
			kv.mu.Unlock()
			// panic("")
			logEntry := Op{
				Type:       "Reconfig",
				Value:      "",
				Key:        "",
				ClientId:   0,
				Sequence:   0,
				Term:       0,
				WrongGroup: false,
				ConfigInfo: temp,
			}
			kv.rf.Start(logEntry)
		} else {

			//newShards := make(map[int]int)
			//currentShards := make(map[int]int)
			data := make(map[int]map[string]string)
			newShardsOnEachGroup := make(map[int][]int)

			//// 找到新的Config里Shards和Gid的对应关系
			//for shardId, Gid := range config.Shards {
			//	newShards[shardId] = Gid
			//}
			//

			//for shardId, Gid := range kv.config.Shards {
			//	currentShards[shardId] = Gid
			//}

			// 寻找不再负责的shard
			for sid, gid := range kv.config.Shards {
				if gid == kv.gid {
					continue
				}
				if kv.shardState[sid] == MOVINGOUT {
					// kv.shardState[sid] = MOVINGOUT
					// kv.groupNeedPush[newGid] = true
					newShardsOnEachGroup[gid] = append(newShardsOnEachGroup[gid], sid)
				}
			}

			// 把迁出的Shards Push出去
			for k, v := range kv.data {
				shardId := key2shard(k)
				gid := kv.config.Shards[shardId]
				// 对应不再负责的kv pair
				if kv.shardState[shardId] == MOVINGOUT {
					// panic("")

					if data[gid] == nil {
						data[gid] = make(map[string]string)
					}

					data[gid][k] = v

				}
			}
			tempConfig := kv.config
			tempSequenceOnEachClient := kv.sequenceOnEachClient

			kv.mu.Unlock()
			for gid, sidVector := range newShardsOnEachGroup {
				if len(sidVector) == 0 || gid == kv.gid {
					continue
				}
				go SendingShardsToOtherGroup(kv, gid, data[gid], tempConfig, newShardsOnEachGroup[gid], tempSequenceOnEachClient)
			}

		}

	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int,
	masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.persister = persister
	kv.data = make(map[string]string)
	kv.sequenceOnEachClient = make(map[int64]int)
	kv.configClerk = shardmaster.MakeClerk(masters)
	//kv.sequence = 1
	// kv.configClerk.Query(-1)
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg, 2)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rpcChannel = make(map[int]chan Op)
	kv.shardState = make([]int, 10)
	for idx, _ := range kv.shardState {
		kv.shardState[idx] = INITIAL
	}
	kv.config = shardmaster.Config{Num: -1}
	DPrintf("Group %v Starts!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", kv.gid)
	go DaemonOfApplying(kv)
	go DaemonOfReconfig(kv)
	return kv
}

// snapshot应该包括当前index
func SnapShot(kv *ShardKV, index int) {
	// panic("")
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= Max(kv.maxraftstate-5, 1) {
		DPrintf("server starts snapshot! me = %v(%v), Logsize = %v", kv.gid, kv.me, kv.persister.RaftStateSize())
		// DPrintf("Snapshot!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11")
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		var err error
		err = e.Encode(kv.data)
		if err != nil {
			panic("kv Snapshot error 1" + err.Error())
		}

		err = e.Encode(kv.sequenceOnEachClient)
		if err != nil {
			panic("kv Snapshot error 2" + err.Error())
		}

		err = e.Encode(kv.config)
		if err != nil {
			panic("kv Snapshot error 3" + err.Error())
		}

		err = e.Encode(kv.shardState)
		if err != nil {
			panic("kv Snapshot error 4" + err.Error())
		}

		err = e.Encode(kv.groupNeedPush)
		if err != nil {
			panic("kv Snapshot error 5" + err.Error())
		}

		err = e.Encode(kv.remainingPull)
		if err != nil {
			panic("kv Snapshot error 6" + err.Error())
		}

		Snapshot := w.Bytes()
		kv.rf.ApplySnapshot(Snapshot, index)
		DPrintf("server starts snapshot over! me = %v(%v), Logsize = %v, leader = %v", kv.gid, kv.me, kv.persister.RaftStateSize(), kv.rf.VoteFor)
	}
}
