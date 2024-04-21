package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"../labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
		// DPrintf("key[0]=%v,shard = %v", key[0], shard)
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	mu       sync.Mutex
	clinetId int64
	Sequence int

	// You will have to modify this struct.
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.Sequence = 1
	ck.clinetId = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//

type getChanReply struct {
	ok    bool
	reply GetReply
}

func SendingGetRpc(srv *labrpc.ClientEnd, args GetArgs, channel chan getChanReply) {
	var temp getChanReply
	var reply GetReply

	ok := srv.Call("ShardKV.Get", &args, &reply)
	temp.ok = ok
	temp.reply = reply
	channel <- temp
}
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				// DPrintf("client is going to get")
				tempChan := make(chan getChanReply)
				// response := getChanReply{}
				go SendingGetRpc(srv, args, tempChan)
				select {
				case response := <-tempChan:
					ok = response.ok
					reply = response.reply
				case <-time.After(TIMEOUT * 2 * time.Millisecond):
					ok = false
				}
				// DPrintf("client hot getans")
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

type putAppendChanReply struct {
	ok    bool
	reply PutAppendReply
}

func SendingPutAppendRpc(srv *labrpc.ClientEnd, args PutAppendArgs, channel chan putAppendChanReply) {
	var temp putAppendChanReply
	var reply PutAppendReply

	ok := srv.Call("ShardKV.PutAppend", &args, &reply)
	temp.ok = ok
	temp.reply = reply
	channel <- temp
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	ck.mu.Lock()
	args.ClientId = ck.clinetId
	args.Sequence = ck.Sequence
	ck.Sequence++
	ck.mu.Unlock()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf("client is going to putAppend")
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				tempChan := make(chan putAppendChanReply)
				// response := getChanReply{}
				go SendingPutAppendRpc(srv, args, tempChan)
				select {
				case response := <-tempChan:
					ok = response.ok
					reply = response.reply
				case <-time.After(TIMEOUT * 2 * time.Millisecond):
					ok = false
				}
				DPrintf("client hot putappendans")
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

//
//func (ck *Clerk) PushShards(configNum int, data map[string]string, gid int) {
//	args := &PushShardsArgs{
//		ConfigNum: configNum,
//		Data:      data,
//	}
//	ck.mu.Lock()
//	args.ClientId = ck.clinetId
//	args.Sequence = ck.Sequence
//	ck.Sequence++
//	ck.mu.Unlock()
//
//	for {
//		if servers, ok := ck.config.Groups[gid]; ok {
//			for si := 0; si < len(servers); si++ {
//				srv := ck.make_end(servers[si])
//				var reply PutAppendReply
//				ok := srv.Call("ShardKV.PushShards", &args, &reply)
//				if ok && reply.Err == OK {
//					return
//				}
//				if ok && reply.Err == ErrWrongGroup {
//					break
//				}
//				// ... not ok, or ErrWrongLeader
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//		// ask master for the latest configuration.
//		ck.config = ck.sm.Query(-1)
//	}
//}
