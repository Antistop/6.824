package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"       //Client请求的key存在
	ErrWrongLeader = "ErrWrongLeader" //Client请求的节点不是Leader
	ErrTimeout     = "ErrTimeout"	  //超时
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64	//client的唯一id
	CommandId int	//命令的唯一id
}

//返回可不可以返回一个LeaderId
type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId   int64
	CommandId  int
}

type GetReply struct {
	Err   Err
	Value string
}
