package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type store interface {
	Get(key string) (value string, ok bool)
	Put(key string, value string) (newValue string)
	Append(key string, args string) (newValue string)
}

//Server传给raft的Start方法的命令结构Command interface{}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandType  string
	Key			 string
	Value 		 string
	ClientId 	 int64
	CommandId	 int
}

//Server的结构体
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big 如果日志增长过大则快照

	// Your definitions here.
	//？？？？？？LSM 数据存在内存？？？
	kvDataBase     KvDataBase
	storeInterface store						//服务器数据库存储（key,value），可自行定义和更换，我们这里抽象成接口（goLang写法）
	replyChMap     map[int]chan ApplyNotifyMsg 	//ApplyNotifyMsg是chan的类型，Leader回复给客户端的响应结果的map（日志Index,ApplyNotifyMsg）
	clientReply    map[int64]CommandContext   	//一个能记录每一个客户端最后一次操作序号和应用结果的map（clientId，CommandContext）
	lastApplied    int							//上一条应用的log的index，防止快照导致回退
}

type KvDataBase struct {
	KvData map[string]string //实际存放数据的map
}

//对数据的map的操作的三个方法Get、Put、Append
func (kv *KvDataBase) Get(key string) (value string, ok bool) {
	value, ok = kv.KvData[key]
	if ok {
		return value, ok
	}
	//若不存在
	return "", ok
}

func (kv *KvDataBase) Put(key string, value string) (newValue string) {
	//往map里面插入或更新元素value
	kv.KvData[key] = value
	return value
}

func (kv *KvDataBase) Append(key string, arg string) (newValue string) {
	value, ok := kv.KvData[key]
	if ok {
		//若存在，直接append附加arg
		newValue := value + arg
		kv.KvData[key] = newValue
		return newValue
	}
	//如果不存在，则直接put arg进去
	kv.KvData[key] = arg
	return arg
}

//可以表示GetReply和PutAppendReply
type ApplyNotifyMsg struct {
	Err Err
	Value string
	//该被应用的command的term,便于RPC handler判断是否为过期请求
	//之前为leader并且start了,但是后来没有成功commit就变成了follower,导致一开始Start()得到的index处的命令不一定是之前的那个,所以需要拒绝掉;
    //或者是处于少部分分区的leader接收到命令,后来恢复分区之后,index处的log可能换成了新的leader的commit的log了
	Term int
}

type CommandContext struct {
	Command int 			//该client目前的commandId
	Reply ApplyNotifyMsg 	//该command的响应
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer func() {
		//log.Printf("kvserver[%d]: 返回Get RPC请求,args=[%v];Reply=[%v]\n", kv.me, args.CommandId, reply.Err)
	}()
	//log.Printf("kvserver[%d]: 接收Get RPC请求,args=[%v]\n", kv.me, args.CommandId)
	//1、先判断该命令是否已经被执行过了(*********如果没有执行过不代表，真的没有执行，可能正在共识，还没有设置clientReply)
	commandContext, ok := kv.clientReply[args.ClientId]
	//这个地方只是做一个判断处理，其实只要ApplyCommand做了幂等性处理就可以了
	if ok && commandContext.Command == args.CommandId {
		//若当前的请求已经被执行过了，并且是最近完成的请求(clientReply中有请求结果)那么直接返回接回
		reply.Err = commandContext.Reply.Err
		reply.Value = commandContext.Reply.Value
		kv.mu.Unlock()
		return
	} else if commandContext.Command > args.CommandId {
		//******如果是等停协议，不会存在这一种情况，因为之前的请求如果没有结束是不会有现在这个请求的，所以不会存在commandId小于的情况
		//以前已经执行过了，所以这个请求时不合法的，返回一个正常的使得客户端退出循环，在当前的实验(等停协议)是没有出现这种情况
		//********如果允许一个客户端并发的情况下就需要额外考虑（论文有提到）
		reply.Err = ErrNoKey
		reply.Value = ""
		log.Printf("到底会不会出现")
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	//2、若命令未被执行，那么开始生成Op并传递Raft
	op := Op {
		CommandType : "Get",
		Key : args.Key,
		ClientId : args.ClientId,
		CommandId : args.CommandId,
	}
	//raft的Start()方法，加入一条日志条目
	//*****在目前的实现中，读（Get）请求也会生成一条 raft 日志去同步，最简单粗暴的方式保证线性一致性，即LogRead方法（论文6.4）
	//但是，这样子实现的读性能会相当的差，实际生产级别的 raft 读请求实现一般都采用了 Read Index 或者 Lease Read 的方式
	//具体原理可以参考此博客：https://tanxinyu.work/consistency-and-consensus/#etcd-%E7%9A%84-Raft
	//具体实现可以参照 SOFAJRaft 的实现博客：https://www.sofastack.tech/blog/sofa-jraft-linear-consistent-read-implementation/
	index, term, isLeader := kv.rf.Start(op)
	//3、若不为Leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//创建一个类型为ApplyNotifyMsg长度为1的chan
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	//log.Printf("kvserver[%d]: 创建reply通道:index=[%d]\n", kv.me, index)
	kv.mu.Unlock()
	//4、等待应用后的返回消息
	//select不是switch，该select语句让 goroutine 等待多个通信操作，select阻塞直到它的一个 case 可以运行
	//然后它执行那个case。如果多个准备就绪，它会随机选择一个，如果没有其他案例准备就绪，则运行default中的案例
	//select使用default案例尝试发送或接收而不阻塞。这里的话就是等待两个case直到有一个case可以运行
	//？？？？？？？？？？java如何实现(select和chan)
	select {
	case replyMsg := <- replyCh:
		//当被通知时，返回结果
		//log.Printf("kvserver[%d]: 获取到通知结果,index=[%d]\n", kv.me, index)
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
			reply.Value = replyMsg.Value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(500 * time.Millisecond):
		//*********如果Raft层长时间无法完成共识(由于网络分区等原因)不要让Server一直阻塞，使其重新选择另一台Server重试
		//log.Printf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op)
		reply.Err = ErrTimeout
	}
	//5、清除chan
	go kv.CloseChan(index)
}

func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.replyChMap[index]
	if !ok {
		//若该index没有保存通道，直接结束
		//log.Printf("kvserver[%d]: 无该通道index: %d\n", kv.me, index)
		return
	}
	close(ch)
	delete(kv.replyChMap, index)
	//log.Printf("kvserver[%d]: 成功删除通道index: %d\n", kv.me, index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer func() {
		//log.Printf("kvserver[%d]: 返回PutAppend RPC请求,args=[%v];Reply=[%v]\n", kv.me, args.CommandId, reply.Err)
	}()
	//log.Printf("kvserver[%d]: 接收PutAppend RPC请求,args=[%v]\n", kv.me, args.CommandId)
	//1、先判断该命令是否已经被执行过了
	commandContext, ok := kv.clientReply[args.ClientId]
	if ok && commandContext.Command == args.CommandId {
		reply.Err = commandContext.Reply.Err
		//log.Printf("kvserver[%d]: CommandId=[%d]==CommandContext.CommandId=[%d] ,直接返回: %v\n", kv.me, args.CommandId, commandContext.Command, reply)
		kv.mu.Unlock()
		return
	} else if commandContext.Command > args.CommandId {
		//以前已经执行过了，所以这个请求时不合法的(客户端路由Leader的请求)，返回一个正常的使得客户端退出循环
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//2、若命令没有被执行，那么开始生成Op并传递给raft
	op := Op {
		CommandType : args.Op,
		Key : args.Key,
		Value : args.Value,
		ClientId : args.ClientId,
		CommandId : args.CommandId,
	}
	index, term, isLeader := kv.rf.Start(op)
	//3、若不为Leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	//log.Printf("kvserver[%d]: 创建reply通道:index=[%d]\n", kv.me, index)
	kv.mu.Unlock()
	//4、等待应用后返回消息
	select {
	case replyMsg := <-replyCh:
		//log.Printf("kvserver[%d]: 获取到通知结果,index=[%d]\n", kv.me, index)
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(500 * time.Millisecond):
		//超时，返回结果，但是不更新Command -> Reply
		reply.Err = ErrTimeout
		log.Printf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op)
	}
	go kv.CloseChan(index)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ReceiveApplyMsg(){
	for !kv.killed() {
		//select阻塞直到它的一个case可以运行
		select {
		case applyMsg := <- kv.applyCh:
			//log.Printf("kvserver[%d]: 获取到applyCh中新的applyMsg=[%v]\n", kv.me, applyMsg.CommandIndex)
			//当为正常的日志条目
			if applyMsg.CommandValid {
				//合法日志条目就应用日志
				kv.ApplyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {
				//合法快照就应用快照
				kv.ApplySnapshot(applyMsg)
			} else {
				//非法消息
				log.Printf("kvserver[%d]: error applyMsg from applyCh: %v\n", kv.me, applyMsg)
			}
		}
	}
}

func (kv *KVServer) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var commonReply ApplyNotifyMsg
	//此语句断言接口值Command包含具体类型Op，并将基础Op值分配给变量op，加括号是接口赋值写法
	op := applyMsg.Command.(Op)
	commandContext, ok := kv.clientReply[op.ClientId]
	//************当命令已经被应用过了：在这个地方就是关键的幂等性(论文6.3)(因为会出现Leader还没有回复客户端就宕机了)
	if ok && commandContext.Command == op.CommandId {
		//log.Printf("kvserver[%d]: 该命令已被应用过,applyMsg: %v, commandContext: %v\n", kv.me, applyMsg, commandContext)
		commonReply = commandContext.Reply
		return
	}
	//当命令未被应用过
	if op.CommandType == "Get" {
		value, ok := kv.storeInterface.Get(op.Key)
		if ok {
			//有数据时
			commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
		} else {
			//没有数据，value就等于""
			commonReply = ApplyNotifyMsg{ErrNoKey, value, applyMsg.CommandTerm}
		}
	} else if op.CommandType == "Put" {
		//被迫返回值，是因为goLang的傻逼语法，不能传给value赋值nil
		value := kv.storeInterface.Put(op.Key, op.Value)
		commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
	} else if op.CommandType == "Append" {
		newValue := kv.storeInterface.Append(op.Key, op.Value)
		commonReply = ApplyNotifyMsg{OK, newValue, applyMsg.CommandTerm}
	}
	//通知handler（处理者）去响应请求
	replyCh, ok := kv.replyChMap[applyMsg.CommandIndex]
	if ok {
		//log.Printf("kvserver[%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", kv.me, op, applyMsg.CommandIndex)
		replyCh <- commonReply
		//log.Printf("kvserver[%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", kv.me, op, applyMsg.CommandIndex)
	}
	//"_"表示忽略变量的意思，这里就是忽视返回值
	//value, _ := kv.storeInterface.Get(op.Key)
	//log.Printf("kvserver[%d]: 此时key=[%v],value=[%v]\n", kv.me, op.Key, value)
	//更新clientReply，用于去重(记录了每个client最后一次执行的信息)
	kv.clientReply[op.ClientId] = CommandContext{op.CommandId, commonReply}
	//log.Printf("kvserver[%d]: 更新ClientId=[%d],CommandId=[%d]\n", kv.me, op.ClientId, op.CommandId)
	kv.lastApplied = applyMsg.CommandIndex
	//是否需要snapshot快照
	if kv.needSnapshot() {
		kv.startSnapshot(applyMsg.CommandIndex)
	}
}

//判断当前是否需要进行snapshot(90%则需要快照)
//测试仪将maxraftstate传递给您的StartKVServer()
//maxraftstate指示您的持久化Raft状态的最大允许大小(以字节为单位)(包括日志，但不包括快照)
//您应该将maxraftstate与persister.RaftStateSize()进行比较。每当您的键/值服务器检测到 Raft 状态大小接近此阈值时
//它应该通过调用Raft的Snapshot来保存快照。如果maxraftstate为 -1，则不必进行快照
//maxraftstate适用于Raft传递给persister.SaveRaftState()的GOB编码字节
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	var proportion float32
	proportion = float32(kv.rf.GetRaftStateSize() / kv.maxraftstate)
	return proportion > 0.9
}

//主动开始snapshot(由leader在maxRaftState不为-1,而且目前接近阈值的时候调用)
func (kv *KVServer) startSnapshot(index int) {
	DPrintf("kvserver[%d]: 容量接近阈值,进行快照,rateStateSize=[%d],maxRaftState=[%d]\n", kv.me, kv.rf.GetRaftStateSize(), kv.maxraftstate)
	snapshot := kv.createSnapshot()
	DPrintf("kvserver[%d]: 完成service层快照\n", kv.me)
	//通知Raft进行快照
	go kv.rf.Snapshot(index, snapshot)
}

//生成server的状态的snapshot，生成持久化的数据byte数组(序列化)，这里不要持久化
func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码kv数据(***其实这个就是Raft的log压缩后的样子)
	e.Encode(kv.kvDataBase)
	//编码clientReply(为了去重)
	e.Encode(kv.clientReply)
	snapshotData := w.Bytes()
	return snapshotData
}

//被动应用snapshot
func (kv *KVServer) ApplySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("kvserver[%d]: 接收到leader的快照\n", kv.me)
	if msg.SnapshotIndex < kv.lastApplied {
		DPrintf("kvserver[%d]: 接收到旧的日志,snapshotIndex=[%d],状态机的lastApplied=[%d]\n", kv.me, msg.SnapshotIndex, kv.lastApplied)
		return
	}
	//判断此时有没有竞争
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		//只有日志落后并且需要的日志Leader已经Snapshot了才会发送snapshot
		kv.lastApplied = msg.SnapshotIndex
		//将快照中的service层数据进行加载
		kv.readSnapshot(msg.Snapshot)
		DPrintf("kvserver[%d]: 完成service层快照\n", kv.me)
	}
}

//反序列化byte数组(发送的snapshot) -> 内存
func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvDataBase KvDataBase
	var clientReply map[int64]CommandContext
	if d.Decode(&kvDataBase) != nil || d.Decode(&clientReply) != nil {
		DPrintf("kvserver[%d]: decode error\n", kv.me)
	} else {
		//**********之前的数据就直接覆盖了，kvDataBase和clientReply在反序列化就生成处理了，这里只需要引用指向即可
		kv.kvDataBase = kvDataBase
		kv.clientReply = clientReply
		kv.storeInterface = &kv.kvDataBase
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
//servers[]包含将通过Raft协作以形成容错key/value服务的一组服务器的端口。me是服务器[]中当前服务器的索引
//k/v服务器应该通过底层Raft实现存储快照，Raft实现应该调用persister。SaveStateAndSnapshot()将Raft状态与快照一起自动保存
//当Raft的保存状态超过maxraftstate字节时，k/v服务器应该进行快照，以允许Raft垃圾收集其日志。如果maxraftstate为-1，则不需要快照。
//StartKVServer()必须快速返回，因此它应该为任何长时间运行的工作启动goroutine
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	//创建raft（也就是启动raft）
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDataBase = KvDataBase{make(map[string]string)}
	//接口赋值数据库实例对象（和java不同，java可以实例对象直接调用方法）
	kv.storeInterface = &kv.kvDataBase
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.clientReply = make(map[int64]CommandContext)

	//从快照中读取数据(***********Server不进行持久化，所有的持久化操作都是由Raft层完成)
	kv.readSnapshot(kv.rf.GetSnapshot())

	//循环读取Raft已经应用的日志条目命令（也就是Raft将命令加入chan通道applyCh）
	//Raft会通过applyCh提交日志到状态机中，让状态机应用命令，因此我们需要一个不断接收applyCh中日志的协程，并应用命令
	//且通过commandIndex通知到该客户端的waitApplyCh，Command函数取消阻塞返回给客户端
	go kv.ReceiveApplyMsg()

	return kv
}
