package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
	"log"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clinetId int64
	Sequence int
	mu       sync.Mutex
	// Your data here.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clinetId = nrand()
	ck.Sequence = 1
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				// DPrintf("hi, %v", reply.Config)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	ck.mu.Lock()
	args.ClientId = ck.clinetId
	args.Sequence = ck.Sequence
	ck.Sequence++
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			//panic("")
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.mu.Lock()
	args.ClientId = ck.clinetId
	args.Sequence = ck.Sequence
	ck.Sequence++
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.mu.Lock()
	args.ClientId = ck.clinetId
	args.Sequence = ck.Sequence
	ck.Sequence++
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
