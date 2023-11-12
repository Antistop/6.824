package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
//import "time"
import "crypto/rand"
import "math/big"
//import "log"

//https://blog.csdn.net/weixin_45938441/article/details/125386091 (有源码)
//https://www.cnblogs.com/pxlsdz/p/15685837.html

//****************lab4A，其实就和lab3A一个逻辑
//Shardctrler分片控制器，和lab3的client基本一样，将分片分配给复制组。
//作用：提供高可用的集群配置管理服务(Raft)(线性化+共识)，实现分片的负载均衡，并尽可能少地移动分片
type Clerk struct {
	servers []*labrpc.ClientEnd //服务器的信息
	// Your data here.
	clientId int64
	leaderId int
	commandId int
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
	// Your code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.commandId = 0
	return ck
}

//查询指定配置号的Config信息
//参数是一个配置号
func (ck *Clerk) Query(num int) Config {
	requestId := ck.commandId + 1
	server := ck.leaderId
	args := QueryArgs{
		Num : num,
		ClientId : ck.clientId,
		CommandId : requestId,
	}
	// Your code here.
	//log.Printf("client[%d]: 开始发送Query RPC;args=[%v]\n", ck.clientId, args.CommandId)
	for {
		reply := QueryReply{}
		ok := ck.servers[server].Call("ShardCtrler.Query", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			server = (server + 1) % len(ck.servers)
			continue
		}
		ck.leaderId = server
		ck.commandId = requestId
		//log.Printf("client[%d]: 发送Query RPC到server[%d]成功,Reply=[%v]\n", ck.clientId, server, reply.Err)
		if reply.Err == OK {
			return reply.Config
		}
	}
}

//添加新的Group副本组
//参数是一组从唯一的非零副本组标识符(GID)到服务器名称列表的映射
func (ck *Clerk) Join(servers map[int][]string) {
	requestId := ck.commandId + 1
	server := ck.leaderId
	args := JoinArgs{
		Servers : servers,
		ClientId : ck.clientId,
		CommandId : requestId,
	}
	// Your code here.
	//log.Printf("client[%d]: 开始发送Join RPC;args=[%v]\n", ck.clientId, args.CommandId)
	for {
		reply := JoinReply{}
		ok := ck.servers[server].Call("ShardCtrler.Join", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			server = (server + 1) % len(ck.servers)
			continue
		}
		ck.leaderId = server
		ck.commandId = requestId
		//log.Printf("client[%d]: 发送Join RPC到server[%d]成功,Reply=[%v]\n", ck.clientId, server, reply.Err)
		if reply.Err == OK {
			return
		}
	}
}

//删除副本组
//参数是以前加入的组的 GID 列表
func (ck *Clerk) Leave(gids []int) {
	requestId := ck.commandId + 1
	server := ck.leaderId
	args := LeaveArgs{
		GIDs : gids,
		ClientId : ck.clientId,
		CommandId : requestId,
	}
	// Your code here.
	//log.Printf("client[%d]: 开始发送Leave RPC;args=[%v]\n", ck.clientId, args.CommandId)
	for {
		reply := LeaveReply{}
		ok := ck.servers[server].Call("ShardCtrler.Leave", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			server = (server + 1) % len(ck.servers)
			continue
		}
		ck.leaderId = server
		ck.commandId = requestId
		if reply.Err == OK {
			return
		}
	}
}

//在副本组之间移动分片，为指定的分片分配指定的组
//参数是分片编号和GID，将数据库子集Shard分配给GID的Group
func (ck *Clerk) Move(shard int, gid int) {
	requestId := ck.commandId + 1
	server := ck.leaderId
	args := MoveArgs{
		Shard : shard,
		GID : gid,
		ClientId : ck.clientId,
		CommandId : requestId,
	}
	// Your code here.
	//log.Printf("client[%d]: 开始发送Move RPC;args=[%v]\n", ck.clientId, args.CommandId)
	for {
		reply := MoveReply{}
		ok := ck.servers[server].Call("ShardCtrler.Move", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			server = (server + 1) % len(ck.servers)
			continue
		}
		ck.leaderId = server
		ck.commandId = requestId
		if reply.Err == OK {
			return
		}
	}
}
