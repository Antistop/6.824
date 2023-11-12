package shardctrler

//
// Shard controler: assigns shards to replication groups.
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
//分片的数量
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
//****一个Config(配置)描述了一组副本组，副本组负责每个分片，分片到组的分配，配置已编号
//*******************副本组代表的就是负责的分片的集群服务器(一个Leader，多个副本)，Groups就代表了，这些集群服务器存在那个机器上面
//*****它其实存放得就是一系列版本的配置信息，最新的下标对应的最新的配置
//一个组可以用来处理一个或多个分片，但是一个分片只能由一个组进行处理，也因此加入组只要不是超过分片的数量，加入的组可以使系统的性能达到提升
type Config struct {
	Num    int              // config number Config编号(版本号)，Num=0表示configuration无效，gid=0也是无效组，边界条件
	Shards [NShards]int     // shard -> gid  分片位置信息，Shards[3] = 2说明分片序号为3的分片负责的集群是Group2(gid = 2)
	Groups map[int][]string // gid -> servers[] 集群成员信息，每个组对应了集群服务器映射名称列表（也就是组信息）
							//Groups[3] = {"ip1","ip2"}说明gid = 3的集群Group3包含两名名称为ip1和ip2机器(实验规定名称用来RPC通信)
}

const (
	OK = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout = "ErrTimeout"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientId int64
	CommandId int
}

type JoinReply struct {
	//WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ClientId int64
	CommandId int
}

type LeaveReply struct {
	//WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientId int64
	CommandId int
}

type MoveReply struct {
	//WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number 所需配置编号
	ClientId int64
	CommandId int
}

type QueryReply struct {
	//WrongLeader bool
	Err         Err
	Config      Config
}
