package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
// import (
// 	"log"
// )

//https://www.cnblogs.com/pxlsdz/p/15640462.html Raft博士论文
//https://zhuanlan.zhihu.com/p/524566276 (有源码) https://github.com/TheR1sing3un/mit-6.824/tree/master/src/kvraft
//https://www.cnblogs.com/pxlsdz/p/15595836.html


//强一致性的解释如下：对于单个请求，整个服务需要表现得像个单机服务，并且对状态机的修改基于之前所有的请求。
//对于并发的请求，返回的值和最终的状态必须相同，就好像所有请求都是串行的一样。即使有些请求发生在了同一时间，那么也应当一个一个响应。
//此外，在一个请求被执行之前，这之前的请求都必须已经被完成（在技术上我们也叫着线性化（linearizability））
//******线性化对于应用程序来说很方便，因为它是您从一次处理一个请求的单个服务器上看到的行为
//例如，如果一个客户端从服务获得更新请求的成功响应，则保证随后从其他客户端启动的读取可以看到该更新的效果
//对于单个服务器来说，提供线性化能力相对容易。如果服务是复制的，那就更难了，因为所有服务器必须为并发请求选择相同的执行顺序
//必须避免使用不是最新的状态回复客户端，并且必须在失败后(产生选举)以一种保留状态的方式恢复它们的状态所有确认的客户端更新

//可以理解成Client的结构体
type Clerk struct {
	servers []*labrpc.ClientEnd  //服务器的信息
	// You will have to modify this struct.
	//可以清楚 KVServer 通过 clientId 可以知道 Client 的请求来自具体哪个客户端，同时保存每个客户端的请求信息和状态
	//所以每个客户端请求过来时，都赋予了一个刚生成的唯一ID，并且同一个请求对应唯一的序列号（clientId），这两个 ID 就可以确定唯一性请求
	//论文6.3(线性化)
	clientId  int64
	commandId int
	leaderId  int
}

//生成唯一id
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.commandId = 0
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
//获取键的当前值，如果密钥不存在，则返回""
func (ck *Clerk) Get(key string) string {
	//保持重复表小的想法，每个客户端一个表条目(在这里就是map中的一对kv)，而不是每个 RPC 一个
	//***每个客户一次只有一个未完成的 RPC请求，每个客户端按顺序编号 RPC请求，当服务器收到客户端 RPC #10 时
	//它可以忘记客户的较低条目，因为这意味着客户端永远不会重新发送旧的RPC(Raft讲座实验三)，和MySQL一样使用的是等停协议
	//***像这种每个客户一次只有一个未完成的RPC请求，其实不需要这种递增的commandId，只需要一个唯一的commandId即可
	//因为每次只会有一个请求，如果服务端的重复表就只需要记录每个客户端最近完成的这个唯一id(nrand()生成)即可(TensShinet)
	//*********因为如果当前请求没有完成，后面的请求是不会执行的，所以到了后面的请求是不会因为网络原因出现之前的请求(除非并发)
	
	//***如果可以并发发送请求，其实不就和TCP一样吗，可能会出现后面commandId的先到，和TCP的情况一样，但是TCP只是数据，可以先保存等待
	//但是我们这个是一致性的一个请求，需要响应客户端的，总不可能等到commandId前面的请求吧，所以需要向TCP一样进行处理
	//这里的想法可以是：所以假如客户端并发发送commandId(1-10)，一致性的顺序取决的是谁先到达服务端并且执行成功(包括共识)
	//然后之前的commandId到达后就只能返回错误，然后commandId再赋值11继续发送RPC，服务端就还是一样只需要记录最后一个执行的commandId
	//再者其实这种类似等停协议，如果当前请求不能成功，那后面的请求不也是和前面一样的形式发送，也说不准会成功，但是并发肯定会更复杂

	//或者论文6.3中也有提到客户端并发：和TCP一样的处理方式，commandId不代表服务器的一致性顺序
	//客户端的会话(可以理解成服务器)不只跟踪客户端最近的序号和响应，而是包含了一组序号和响应(1-10)，重复表会很大(讲座)
	//客户端每次请求都将包括其尚未收到响应的最低序列号(例如1之前的)，然后状态机将丢弃所有比之较低的序列号响应
	//因为如果后面的commandId执行成功了，是不会再出现的，所以只需要记住最低序列号即可(类似TCP)
	//如果有一个迟迟没有成功的话，后面的请求还是会创建重复表，例如4没成功，重复表就是[4,10+]，即使后面的请求执行成功了，也会保存
	requestId := ck.commandId + 1
	//第一个发送的目标server是上一次RPC发现的leader
	server := ck.leaderId
	args := GetArgs {
		Key : key,
		ClientId : ck.clientId,
		CommandId : requestId,
	}
	//log.Printf("client[%d]: 开始发送Get RPC;args=[%v]\n", ck.clientId, args.CommandId)
	//直到发送成功为止
	for {
		reply := GetReply{}
		//RPC请求server
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		//请求服务端超时或请求的服务端不为Leader时，尝试连接其他服务端
		//论文6.2（将请求路由到Leader的方案）
		//*********如果Raft层长时间无法完成共识(由于网络分区等原因)不要让Server一直阻塞，使其重新选择另一台Server重试
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			//log.Printf("client[%d]: 发送Get RPC到server[%d]失败,ok = %v,Reply=[%v]\n", ck.clientId, server, ok, reply.Err)
			//换一个节点
			server = (server + 1) % len(ck.servers)
			continue
		}
		//log.Printf("client[%d]: 发送Get RPC到server[%d]成功,Reply=[%v]\n", ck.clientId, server, reply.Value)
		//更新最近发现的Leader
		ck.leaderId = server
		//请求成功，请求id++
		ck.commandId = requestId
		//请求的key不存在
		if reply.Err == ErrNoKey {
			return ""
		}
		//请求的key存在
		if reply.Err == OK {
			return reply.Value
		}
	}

	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
//由Put和Append共享
//Put替换数据库中特定键的值，Append将value附加到值，对于不存在的键，Put就是加入操作，Append应该像Put一样也是加入
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	requestId := ck.commandId + 1
	server := ck.leaderId
	args := PutAppendArgs {
		Key : key,
		Value : value,
		Op : op,
		ClientId : ck.clientId,
		CommandId : requestId,
	}
	//log.Printf("client[%d]: 开始发送PutAppend RPC;args=[%v]\n", ck.clientId, args.CommandId)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			//log.Printf("client[%d]: 发送PutAppend RPC到server[%d]失败,ok = %v,Reply=[%v]\n", ck.clientId, server, ok, reply.Err)
			server = (server + 1) % len(ck.servers)
			continue
		}
		//log.Printf("client[%d]: 发送PutAppend RPC到server[%d]成功,Reply=[%v]\n", ck.clientId, server, reply.Err)
		ck.leaderId = server
		ck.commandId = requestId
		if reply.Err == OK {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
