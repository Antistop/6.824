package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"
// import "log"

//https://blog.csdn.net/weixin_45938441/article/details/125566763 主要参考博客(该博客的bug已解决)
//https://gitee.com/GeminiCx/lab_6824/tree/master/src/shardkv 源码
//下面参考思路和解析
//https://blog.csdn.net/qq_40443651/article/details/118034894 
//https://www.cnblogs.com/pxlsdz/p/15685837.html

//lab3就是lab4的一个铺垫，和lab4没有关系，lab3就是没有分片的KVServer，lab4就是分片的KVServer

//用于与分片K/V服务通信的客户端代码
//********一个组就是一个基于Raft共识的集群，这个组负责的分片全部存储在集群当中
//********客户机首先与shardctrler对话，以找出键所在的分片是哪个组，然后与持有键的分片的组对话
//client客户端，将发送的请求利用Key2Shard进行分片，分到具体某个组下的server，然后这个server如果是leader
//则再利用自身的raft组进行共识，利用共识对整个server集群同步当前组对分片的操作，保持容错，而整个系统的集群则是通过lab4A的分片控制器来保证

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
//根据key来路由到具体的shard分片(*********************按范围进行分片)
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		//ASCII码和java一样 char -> int
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk  			 //分片控制器
	config   shardctrler.Config				 //副本组的配置信息
	make_end func(string) *labrpc.ClientEnd  //服务器信息
	// You will have to modify this struct.
	clientId int64
	//leaderId int  //这里根据实验给我们提示的代码，我们就没有实现leaderId了，按照之前的传统去记录下leaderId这样WrongLeader的次数就会变少
	commandId int
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
//make_end(servername)将服务器名称从Config.Groups[gid][i]转换为labrpc.ClientEnd，您可以在其上发送 RPC
//意思就是表示server服务器信息，用来向服务器发送RPC，前面lab的client都有
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	//创建分片控制器
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.commandId = 0
	//去分片控制器上查到最新的副本组配置
	ck.config = ck.sm.Query(-1)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	requestId := ck.commandId + 1
	args := GetArgs{
		Key : key,
		ClientId : ck.clientId,
		CommandId : requestId,
	}
	//log.Printf("client[%d]: 开始发送Get RPC;args=[%v]\n", ck.clientId, args.CommandId)
	for {
		//根据key来路由到具体的shard分片
		shard := key2shard(key)
		//根据分片来找到具体的Group副本组
		gid := ck.config.Shards[shard]
		//拿到副本组中的所有集群成员信息servers(这些集群服务器就构成了一个集群(Raft实现了共识))
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				//将服务器名字转成服务器信息进行RPC调用（实验规则）
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.commandId = requestId
					return reply.Value
				}
				//该分片已经转移到其他的副本组
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
				//如果是ShardNotArrived分片还没有迁移完成，那就可以睡一会在发送(lab3那种写法更方便编写，我这里就不写了)
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		//向控制器询问最新配置，如果请求不合法，在客户端会拿到最新的配置(解决了有些服务器组不是最新的Config配置(Num))
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestId := ck.commandId + 1
	args := PutAppendArgs{
		Key : key,
		Value : value,
		Op : op,
		ClientId : ck.clientId,
		CommandId : requestId,
	}
	//log.Printf("client[%d]: 开始发送PutAppend RPC;args=[%v]\n", ck.clientId, args.CommandId)
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.commandId = requestId
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		//log.Printf("client配置 %d", ck.config.Num)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
