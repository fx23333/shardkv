package shardmaster

import (
	"../raft"
	"bytes"
	"sort"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

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

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	sequenceOnEachClient map[int64]int
	rpcChannel           map[int]chan Op // log index -> the channel of a RPC

	configs      []Config // indexed by config num
	maxraftstate int
	persister    *raft.Persister
}

type Op struct {
	// Your data here.
	Type       int
	ClientId   int64
	Sequence   int
	Join       JoinArgs
	Leave      LeaveArgs
	Move       MoveArgs
	Query      QueryArgs
	ConfigInfo Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	logEntry := Op{Type: JOIN, Join: *args, ClientId: args.ClientId, Sequence: args.Sequence}
	index, _, isLeader := sm.rf.Start(logEntry)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	// DPrintf("Recevie Join, args = %v", args)
	sm.rpcChannel[index-1] = make(chan Op, 1)
	waitingChannel := sm.rpcChannel[index-1]
	sm.mu.Unlock()
	select {
	case logEntryReturnedFromRaft := <-waitingChannel:
		if logEntryReturnedFromRaft.ClientId != logEntry.ClientId ||
			logEntryReturnedFromRaft.Sequence != logEntry.Sequence {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
		} else {
			reply.Err = OK
			reply.WrongLeader = false
			//--DPrintf("Get(%v) From Server(%v) OK:%v", args.Key, kv.me, logEntryReturnedFromRaft.Value)
		}
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrWrongLeader
		//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
	}
	sm.mu.Lock()
	delete(sm.rpcChannel, index-1)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	logEntry := Op{Type: LEAVE, Leave: *args, ClientId: args.ClientId, Sequence: args.Sequence}
	index, _, isLeader := sm.rf.Start(logEntry)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	sm.rpcChannel[index-1] = make(chan Op, 1)
	// DPrintf("Recevie Leave, args = %v", args)
	waitingChannel := sm.rpcChannel[index-1]
	sm.mu.Unlock()
	select {
	case logEntryReturnedFromRaft := <-waitingChannel:
		if logEntryReturnedFromRaft.ClientId != logEntry.ClientId ||
			logEntryReturnedFromRaft.Sequence != logEntry.Sequence {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
		} else {
			reply.Err = OK
			reply.WrongLeader = false
			//--DPrintf("Get(%v) From Server(%v) OK:%v", args.Key, kv.me, logEntryReturnedFromRaft.Value)
		}
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrWrongLeader
		//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
	}
	sm.mu.Lock()
	delete(sm.rpcChannel, index-1)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	logEntry := Op{Type: MOVE, Move: *args, ClientId: args.ClientId, Sequence: args.Sequence}
	index, _, isLeader := sm.rf.Start(logEntry)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	// DPrintf("Recevie Move, args = %v", args)
	sm.rpcChannel[index-1] = make(chan Op, 1)
	waitingChannel := sm.rpcChannel[index-1]
	sm.mu.Unlock()
	select {
	case logEntryReturnedFromRaft := <-waitingChannel:
		if logEntryReturnedFromRaft.ClientId != logEntry.ClientId ||
			logEntryReturnedFromRaft.Sequence != logEntry.Sequence {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
		} else {
			reply.Err = OK
			reply.WrongLeader = false
			//--DPrintf("Get(%v) From Server(%v) OK:%v", args.Key, kv.me, logEntryReturnedFromRaft.Value)
		}
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrWrongLeader
		//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
	}
	sm.mu.Lock()
	delete(sm.rpcChannel, index-1)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	logEntry := Op{Type: QUERY, Query: *args}
	// DPrintf("args = %v", *args)
	index, _, isLeader := sm.rf.Start(logEntry)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	// DPrintf("Recevie Query, args = %v", args)
	sm.rpcChannel[index-1] = make(chan Op, 1)
	waitingChannel := sm.rpcChannel[index-1]
	sm.mu.Unlock()
	select {
	case logEntryReturnedFromRaft := <-waitingChannel:
		if logEntryReturnedFromRaft.ClientId != logEntry.ClientId ||
			logEntryReturnedFromRaft.Sequence != logEntry.Sequence {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
		} else {
			//panic("")
			reply.Err = OK
			reply.WrongLeader = false
			reply.Config = logEntryReturnedFromRaft.ConfigInfo
			//DPrintf("config:%v", reply.Config)
			//--DPrintf("Get(%v) From Server(%v) OK:%v", args.Key, kv.me, logEntryReturnedFromRaft.Value)
		}
	case <-time.After(TIMEOUT * time.Millisecond):
		reply.Err = ErrWrongLeader
		//--DPrintf("Wrong Server, me = %v, leader = %v\n", kv.me, kv.rf.VoteFor)
	}
	sm.mu.Lock()
	delete(sm.rpcChannel, index-1)
	sm.mu.Unlock()
}

// Kill
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// Raft needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// ApplyJoin 需要在外部上锁

func (sm *ShardMaster) ApplyJoin(args JoinArgs) {
	// panic("")
	var tempNewConfig Config
	newConfigIndex := len(sm.configs)
	currentConfigIndex := newConfigIndex - 1
	numOfOldGroups := len(sm.configs[currentConfigIndex].Groups)
	numOfNewGroups := len(args.Servers)
	var shardsOnOldGroup map[int][]int
	shardsOnOldGroup = make(map[int][]int)
	var numOfTotalGroup int
	numOfTotalGroup = numOfNewGroups + numOfOldGroups

	if numOfOldGroups > 10 {
		for k, v := range sm.configs[currentConfigIndex].Shards {
			tempNewConfig.Shards[k] = v
		}

		tempNewConfig.Groups = make(map[int][]string)
		for k, v := range sm.configs[currentConfigIndex].Groups {
			tempNewConfig.Groups[k] = v
		}
		for k, v := range args.Servers {
			tempNewConfig.Groups[k] = v
		}
		// tempNewConfig.Num = sm.configs[currentConfigIndex].Num + 1
		//
		//panic("")
		tempNewConfig.Num = sm.configs[currentConfigIndex].Num + 1
		sm.configs = append(sm.configs, tempNewConfig)
		DPrintf("%v, New config: %v", currentConfigIndex+1, tempNewConfig)
		return
	}

	numOfShardsOnNewGroups := NShards / numOfTotalGroup
	remainder := NShards % numOfTotalGroup

	var max, min, numOfMax, numOfMin int
	min = numOfShardsOnNewGroups
	numOfMax = remainder
	numOfMin = numOfTotalGroup - numOfMax
	if remainder == 0 {
		max = numOfShardsOnNewGroups
	} else {
		max = numOfShardsOnNewGroups + 1
	}

	var less map[int]int
	less = make(map[int]int)
	var more []int
	// less： 太少数组，用于存储数目不够的Group的Id
	// more: 太多数组，用于存储数目太大的Group的某个Shard的Id

	// 得到当前config下每个Group负责的shard
	for shardIdx, groupResponsibleFor := range sm.configs[currentConfigIndex].Shards {
		shardsOnOldGroup[groupResponsibleFor] = append(shardsOnOldGroup[groupResponsibleFor], shardIdx)
	}

	i := 0
	var groupIdSorted []int
	for groupId, _ := range shardsOnOldGroup {
		groupIdSorted = append(groupIdSorted, groupId)
	}

	sort.Ints(groupIdSorted)
	for _, groupId := range groupIdSorted {
		shardVector := shardsOnOldGroup[groupId]
		Len := len(shardVector)
		temp := 0
		if i < numOfMin {
			// DPrintf("len = %v, temp = %v, min = %v", Len, temp, min)
			for Len+temp < min {
				temp++
				less[groupId]++

			}

			temp = 0
			for Len-temp > min {
				more = append(more, shardVector[temp])
				//panic("")

				temp++
			}
		} else {
			for Len+temp < max {
				temp++
				less[groupId]++
			}

			temp = 0
			for Len-temp > max {
				more = append(more, shardVector[temp])
				//panic("")
				// DPrintf("temp = %v", max)
				temp++
			}
		}
		i++
		// DPrintf("i = %v", i)
	}

	groupIdSorted = make([]int, 0)
	for groupId, _ := range args.Servers {
		groupIdSorted = append(groupIdSorted, groupId)
	}
	sort.Ints(groupIdSorted)
	for _, groupId := range groupIdSorted {
		if i < numOfMin {
			less[groupId] += min
		} else {
			less[groupId] += max
		}
		i++
	}

	for k, v := range sm.configs[currentConfigIndex].Shards {
		tempNewConfig.Shards[k] = v
	}
	// var oneGroupId int
	tempNewConfig.Groups = make(map[int][]string)

	for k, v := range sm.configs[currentConfigIndex].Groups {
		tempNewConfig.Groups[k] = v
	}

	for k, v := range args.Servers {
		tempNewConfig.Groups[k] = v
		//oneGroupId = k
	}

	if numOfOldGroups == 0 {
		more = make([]int, 0)
		for i := 0; i < NShards; i++ {
			more = append(more, i)
		}

	}
	// DPrintf("more = %v", more)
	i = 0
	for groupId, neededShards := range less {
		for j := 0; j < neededShards; j++ {
			if i >= len(more) {
				break
			}
			shardId := more[i]
			tempNewConfig.Shards[shardId] = groupId
			i++
			// DPrintf("i = %v, shards = %v", i, tempNewConfig.Shards)
		}
		// DPrintf("iiiiiiiiii = %v, groupId = %v, needed = %v", i, groupId, neededShards)
	}

	// tempNewConfig.Groups = make(map[int][]string, numOfNewGroups+numOfOldGroups)

	// 将新的groups写道temp中

	// 将temp追加到sm.configs中
	tempNewConfig.Num = sm.configs[currentConfigIndex].Num + 1
	sm.configs = append(sm.configs, tempNewConfig)
	DPrintf("%v: New config: %v", currentConfigIndex+1, tempNewConfig)
}

func (sm *ShardMaster) ApplyLeave(args LeaveArgs) {
	var tempNewConfig Config
	newConfigIndex := len(sm.configs)
	currentConfigIndex := newConfigIndex - 1
	numOfOldGroups := len(sm.configs[currentConfigIndex].Groups)
	numOfDeletedGroups := len(args.GIDs)

	var shardsOnDeletedGroup map[int][]int
	shardsOnDeletedGroup = make(map[int][]int)
	var shardsOnNewGroup map[int][]int
	shardsOnNewGroup = make(map[int][]int)
	numOfTotalGroup := numOfOldGroups - numOfDeletedGroups

	if numOfTotalGroup == 0 {
		for shardId, _ := range tempNewConfig.Shards {
			tempNewConfig.Shards[shardId] = 0
		}
		tempNewConfig.Num = sm.configs[currentConfigIndex].Num + 1

		sm.configs = append(sm.configs, tempNewConfig)
		DPrintf("%v: New config: %v", currentConfigIndex+1, tempNewConfig)
		return
	}
	numOfShardsOnNewGroups := NShards / numOfTotalGroup
	// remainingShards := 10
	remainder := NShards % numOfTotalGroup
	// panic(numOfShardsOnNewGroups)
	for _, groupId := range args.GIDs {
		shardsOnDeletedGroup[groupId] = make([]int, 0)
	}
	for groupId, _ := range sm.configs[currentConfigIndex].Groups {
		_, ok := shardsOnDeletedGroup[groupId]
		if !ok {
			shardsOnNewGroup[groupId] = make([]int, 0)
		}
	}

	// 得到当前config下每个即将被删除的Group / 未被删除的Group  负责的shard
	for shardIdx, groupResponsibleFor := range sm.configs[currentConfigIndex].Shards {
		_, ok := shardsOnDeletedGroup[groupResponsibleFor]
		if !ok {
			_, ok2 := shardsOnNewGroup[groupResponsibleFor]
			if !ok2 {
				shardsOnNewGroup[groupResponsibleFor] = make([]int, 0)
			}

			shardsOnNewGroup[groupResponsibleFor] = append(shardsOnNewGroup[groupResponsibleFor], shardIdx)
			// DPrintf("shards....:%v", shardsOnNewGroup[groupResponsibleFor])
			// panic("")
		} else {
			shardsOnDeletedGroup[groupResponsibleFor] =
				append(shardsOnDeletedGroup[groupResponsibleFor], shardIdx)
		}

	}

	tempNewConfig.Groups = make(map[int][]string, numOfOldGroups-numOfDeletedGroups)

	var max, min, numOfMax, numOfMin int
	min = numOfShardsOnNewGroups
	numOfMax = remainder
	numOfMin = numOfTotalGroup - numOfMax
	if remainder == 0 {
		max = numOfShardsOnNewGroups
	} else {
		max = numOfShardsOnNewGroups + 1
	}
	// DPrintf("%v:, New:%v\ndeleted:%v, min = %v,max = %v",
	//currentConfigIndex+1, shardsOnNewGroup, shardsOnDeletedGroup, min, max)

	var less map[int]int
	less = make(map[int]int)
	var more []int
	// less： 太少数组，用于存储数目不够的Group的Id
	// more: 太多数组，用于存储数目太大的Group的某个Shard的Id

	// 得到当前config下每个Group负责的shard
	//for shardIdx, groupResponsibleFor := range sm.configs[currentConfigIndex].Shards {
	//	shardsOnOldGroup[groupResponsibleFor] = append(shardsOnOldGroup[groupResponsibleFor], shardIdx)
	//}

	i := 0
	groupIdSorted := make([]int, 0)
	for groupId, _ := range shardsOnNewGroup {
		groupIdSorted = append(groupIdSorted, groupId)
	}
	sort.Ints(groupIdSorted)
	for _, groupId := range groupIdSorted {
		//if i >= 10 {
		//	break
		//}
		shardVector := shardsOnNewGroup[groupId]
		Len := len(shardVector)
		temp := 0
		if i < numOfMin {
			for Len+temp < min {
				temp++
				less[groupId]++

			}

			temp = 0
			for Len-temp > min {
				more = append(more, shardVector[temp])
				//panic("")
				// DPrintf("%v, %v", numOfMin, numOfMax)
				temp++
			}
		} else {
			for Len+temp < max {
				temp++
				less[groupId]++
			}

			temp = 0
			for Len-temp > max {
				more = append(more, shardVector[temp])
				//panic("")
				// DPrintf("temp = %v", max)
				temp++
			}
		}
		i++
		// DPrintf("i = %v", i)
	}

	groupIdSorted = make([]int, 0)
	for groupId, _ := range shardsOnDeletedGroup {
		groupIdSorted = append(groupIdSorted, groupId)
	}
	sort.Ints(groupIdSorted)

	for _, groupId := range groupIdSorted {
		shardVector := shardsOnDeletedGroup[groupId]
		for _, shardId := range shardVector {
			more = append(more, shardId)
			// panic("")
		}
	}

	for k, v := range sm.configs[currentConfigIndex].Shards {
		tempNewConfig.Shards[k] = v
	}
	var oneGroupId int
	tempNewConfig.Groups = make(map[int][]string)

	for k, v := range sm.configs[currentConfigIndex].Groups {
		tempNewConfig.Groups[k] = v
	}

	if false {
		for i := 0; i < NShards; i++ {
			tempNewConfig.Shards[i] = oneGroupId
		}
	} else {
		i = 0
		for groupId, neededShards := range less {
			//if i >= len(more) {
			//	break
			//}
			for j := 0; j < neededShards && i < len(more); j++ {
				shardId := more[i]
				tempNewConfig.Shards[shardId] = groupId
				i++
				// panic("")
			}
		}
	}

	//// 将修改后的new groups的shards写到temp中
	//for i, shardVectorOnI := range shardsOnNewGroup {
	//	for _, shardId := range shardVectorOnI {
	//		tempNewConfig.Shards[shardId] = i
	//	}
	//}

	tempNewConfig.Groups = make(map[int][]string)
	//tempNewConfig.Groups = sm.configs[currentConfigIndex].Groups
	for k, v := range sm.configs[currentConfigIndex].Groups {
		tempNewConfig.Groups[k] = v
	}
	// 将新的groups写道temp中
	for k, _ := range shardsOnDeletedGroup {
		delete(tempNewConfig.Groups, k)
	}

	// 将temp追加到sm.configs中
	tempNewConfig.Num = sm.configs[currentConfigIndex].Num + 1
	sm.configs = append(sm.configs, tempNewConfig)
	DPrintf("%v: New config: %v", currentConfigIndex+1, tempNewConfig)
}

//
//func (sm *ShardMaster) aApplyLeave(args LeaveArgs) {
//	var tempNewConfig Config
//	newConfigIndex := len(sm.configs)
//	currentConfigIndex := newConfigIndex - 1
//	numOfOldGroups := len(sm.configs[currentConfigIndex].Groups)
//	numOfDeletedGroups := len(args.GIDs)
//
//	var shardsOnDeletedGroup map[int][]int
//	shardsOnDeletedGroup = make(map[int][]int)
//	var shardsOnNewGroup map[int][]int
//	shardsOnNewGroup = make(map[int][]int)
//
//	if numOfOldGroups-numOfDeletedGroups == 0 {
//		for shardId, _ := range tempNewConfig.Shards {
//			tempNewConfig.Shards[shardId] = 0
//		}
//		tempNewConfig.Num = sm.configs[currentConfigIndex].Num + 1
//
//		sm.configs = append(sm.configs, tempNewConfig)
//		DPrintf("New config: %v", tempNewConfig)
//		return
//	}
//	numOfShardsOnNewGroups := NShards / (numOfOldGroups - numOfDeletedGroups)
//	// remainingShards := 10
//	remainder := numOfShardsOnNewGroups + NShards%(numOfOldGroups-numOfDeletedGroups)
//	// panic(numOfShardsOnNewGroups)
//	for _, groupId := range args.GIDs {
//		shardsOnDeletedGroup[groupId] = make([]int, 0)
//	}
//
//	// 得到当前config下每个即将被删除的Group / 未被删除的Group  负责的shard
//	for shardIdx, groupResponsibleFor := range sm.configs[currentConfigIndex].Shards {
//		_, ok := shardsOnDeletedGroup[groupResponsibleFor]
//		if !ok {
//			shardsOnNewGroup[groupResponsibleFor] = append(shardsOnNewGroup[groupResponsibleFor], shardIdx)
//		} else {
//			shardsOnDeletedGroup[groupResponsibleFor] =
//				append(shardsOnDeletedGroup[groupResponsibleFor], shardIdx)
//		}
//
//	}
//
//	tempNewConfig.Groups = make(map[int][]string, numOfOldGroups-numOfDeletedGroups)
//	DPrintf("New:%v\ndeleted:%v", shardsOnNewGroup, shardsOnDeletedGroup)
//	groupNum := 0
//	for groupId, shardVectorOnDeletedGroup := range shardsOnDeletedGroup {
//
//		remainingShards := len(shardVectorOnDeletedGroup) - 1
//		//panic(remainingShards)
//		for i, _ := range shardsOnNewGroup {
//			if remainingShards <= -1 {
//				break
//			}
//			numberOfShardsOnI := len(shardsOnNewGroup[i])
//			count := 0
//			if groupNum != numOfDeletedGroups-1 && numberOfShardsOnI < numOfShardsOnNewGroups {
//				for ; remainingShards >= 0 && count+numberOfShardsOnI < numOfShardsOnNewGroups; remainingShards-- {
//					shardId := shardVectorOnDeletedGroup[remainingShards]
//					tempNewConfig.Shards[shardId] = i
//					//remainingShards--
//					count++
//				}
//				// panic("")
//			} else if groupNum == numOfDeletedGroups-1 && numberOfShardsOnI < remainder {
//				//panic("")
//				// panic(numberOfShardsOnI)
//				// panic(remainingShards)
//				for ; remainingShards >= 0 && count+numberOfShardsOnI < numOfShardsOnNewGroups; remainingShards-- {
//					shardId := shardVectorOnDeletedGroup[remainingShards]
//					tempNewConfig.Shards[shardId] = i
//					//remainingShards--
//					count++
//					//panic("")
//				}
//			}
//
//			if remainingShards-1 > 0 {
//				shardsOnDeletedGroup[groupId] = shardsOnDeletedGroup[groupId][:remainingShards-1]
//			} else {
//				shardsOnDeletedGroup[groupId] = nil
//			}
//		}
//
//		groupNum++
//	}
//
//	// 将修改后的new groups的shards写到temp中
//	for i, shardVectorOnI := range shardsOnNewGroup {
//		for _, shardId := range shardVectorOnI {
//			tempNewConfig.Shards[shardId] = i
//		}
//	}
//
//	tempNewConfig.Groups = make(map[int][]string)
//	//tempNewConfig.Groups = sm.configs[currentConfigIndex].Groups
//	for k, v := range sm.configs[currentConfigIndex].Groups {
//		tempNewConfig.Groups[k] = v
//	}
//	// 将新的groups写道temp中
//	for k, _ := range shardsOnDeletedGroup {
//		delete(tempNewConfig.Groups, k)
//	}
//
//	// 将temp追加到sm.configs中
//	tempNewConfig.Num = sm.configs[currentConfigIndex].Num + 1
//	sm.configs = append(sm.configs, tempNewConfig)
//	DPrintf("New config: %v", tempNewConfig)
//}

func (sm *ShardMaster) ApplyMove(args MoveArgs) {
	var tempNewConfig Config
	newConfigIndex := len(sm.configs)
	currentConfigIndex := newConfigIndex - 1
	tempNewConfig.Groups = sm.configs[currentConfigIndex].Groups
	tempNewConfig.Groups = make(map[int][]string)

	for k, v := range sm.configs[currentConfigIndex].Groups {
		tempNewConfig.Groups[k] = v
	}
	tempNewConfig.Shards = sm.configs[currentConfigIndex].Shards
	copy(tempNewConfig.Shards[:], sm.configs[currentConfigIndex].Shards[:])

	tempNewConfig.Shards[args.Shard] = args.GID

	// 将temp追加到sm.configs中
	tempNewConfig.Num = sm.configs[currentConfigIndex].Num + 1
	sm.configs = append(sm.configs, tempNewConfig)
}

//func (sm *ShardMaster)ApplyQuery(args *QueryArgs) {
//
//}

func DaemonOfApplying(sm *ShardMaster) {
	for entry := range sm.applyCh {
		sm.mu.Lock()

		if !entry.CommandValid {
			// panic("")
			r := bytes.NewBuffer(entry.Snapshot)
			d := labgob.NewDecoder(r)
			err := d.Decode(&sm.configs)
			if err != nil {
				panic("Server Decode Error!" + err.Error())
			}
			//--DPrintf("data:%v", kv.data)

			err = d.Decode(&sm.sequenceOnEachClient)
			if err != nil {
				panic("Server Decode Error!" + err.Error())
			}
			// panic("")
			sm.mu.Unlock()
			continue
		}
		tempEntry := entry.Command.(Op)

		if entry.Command.(Op).Type != QUERY {

			if sm.sequenceOnEachClient[entry.Command.(Op).ClientId] < entry.Command.(Op).Sequence {
				// panic("")
				switch entry.Command.(Op).Type {

				case JOIN:
					sm.ApplyJoin(entry.Command.(Op).Join)
				case LEAVE:
					sm.ApplyLeave(entry.Command.(Op).Leave)
				case MOVE:
					sm.ApplyMove(entry.Command.(Op).Move)
				default:
					panic("KVSERVER FUNC(Excute): WRONG OPERATION")
				}
				sm.sequenceOnEachClient[entry.Command.(Op).ClientId] = raft.Max(sm.sequenceOnEachClient[entry.Command.(Op).ClientId], entry.Command.(Op).Sequence)
			}

		} else {
			//panic("")
			// tempEntry.Value = kv.data[tempEntry.Key]
			if entry.Command.(Op).Query.Num == -1 || entry.Command.(Op).Query.Num > len(sm.configs)-1 {
				tempEntry.ConfigInfo = sm.configs[len(sm.configs)-1]
			} else {
				tempEntry.ConfigInfo = sm.configs[entry.Command.(Op).Query.Num]
			}
			//panic("I haven't implemented Query!")
		}

		waitingChannelOnRpc, ok := sm.rpcChannel[entry.CommandIndex-1]

		if ok {
			select {
			case waitingChannelOnRpc <- tempEntry:
				// case <-time.After((TIMEOUT / 10) * time.Millisecond):
			}
			// kick their ass
			delete(sm.rpcChannel, entry.CommandIndex-1)
		}

		sm.mu.Unlock()
		SnapShot(sm, entry.CommandIndex-1)
		//panic("I haven't implemented Snapshot Process")
	}
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.maxraftstate = -1
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	// sm.configs[0].Groups[0] = make([]string, 0)
	for idx, _ := range sm.configs[0].Shards {
		sm.configs[0].Shards[idx] = 0
	}
	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg, 2)
	sm.persister = persister
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.rpcChannel = make(map[int]chan Op)
	sm.sequenceOnEachClient = make(map[int64]int)
	// Your code here.
	go DaemonOfApplying(sm)
	return sm
}

// snapshot应该包括当前index
func SnapShot(sm *ShardMaster, index int) {
	if sm.maxraftstate != -1 && sm.persister.RaftStateSize() >= Max(sm.maxraftstate-5, 1) {
		//--DPrintf("server starts snapshot! me = %v, Logsize = %v", kv.me, kv.persister.RaftStateSize())
		//panic("")
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		var err error
		err = e.Encode(sm.configs)
		if err != nil {
			panic("sm Snapshot error 1" + err.Error())
		}

		err = e.Encode(sm.sequenceOnEachClient)
		if err != nil {
			panic("sm Snapshot error 2" + err.Error())
		}
		Snapshot := w.Bytes()
		sm.rf.ApplySnapshot(Snapshot, index)
		//--DPrintf("server starts snapshot over! me = %v, Logsize = %v", kv.me, kv.persister.RaftStateSize())
	}
}
