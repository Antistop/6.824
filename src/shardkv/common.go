package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrTimeout	     = "ErrTimeout"
	ShardNotArrived  = "ShardNotArrived"
	ConfigNotArrived = "ConfigNotArrived"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	CommandId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	CommandId int
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArgs struct {
	ClientReply 		 map[int64]CommandContext	//接收者更新其状态
	ShardId				 int			//分片id
	Shard				 KvDataBase		//要发送的分片
	ClientId			 int64
	CommandId			 int			 
}

type AddShardReply struct {
	Err Err
}