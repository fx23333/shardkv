package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	// GLOBAL CONSTANTS
	TIMEOUT = 100
	JOIN    = 0
	LEAVE   = 1
	MOVE    = 2
	QUERY   = 3

	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// JoinArgs The Join RPC is used by an administrator to add new replica groups.
// Its argument is a set of mappings from unique,
// non-zero replica group identifiers (GIDs) to lists of server names.
// The shardmaster should react by creating a new configuration that includes the new replica groups.
// The new configuration should divide the shards as evenly as
// possible among the full set of groups,
// and should move as few shards as possible to achieve that goal.
// The shardmaster should allow re-use of a GID if it's not part of the current configuration
// (i.e. a GID should be allowed to Join, then Leave, then Join again).

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	Sequence int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	Sequence int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	Sequence int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	ClientId int64
	Sequence int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
