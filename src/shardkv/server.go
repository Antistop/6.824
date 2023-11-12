package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import (
	"6.824/shardctrler"
	"log"
	"time"
	"sync/atomic"
	"bytes"
)

const (
	UpConfigLoopInterval = 100 * time.Millisecond //轮询配置周期
)

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
	UpConfig	 shardctrler.Config
	ShardId		 int
	Shard	 	 KvDataBase
	ClientReply  map[int64]CommandContext
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int		//当前服务器所在的Group副本组
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead    int32 // set by Kill()

	// Your definitions here.					//这里数据库使用的是数组就没有使用接口了(不会goLang语法)
	kvDataBase     []KvDataBase					//******有多个分片就是有多个数据库，如果KvData == nil则说明当前的数据不归当前分片管
	replyChMap     map[int]chan ApplyNotifyMsg 	//ApplyNotifyMsg是chan的类型，Leader回复给客户端的响应结果的map（日志Index,ApplyNotifyMsg）
	clientReply    map[int64]CommandContext   	//一个能记录每一个客户端最后一次操作序号和应用结果的map（clientId，CommandContext）
	lastApplied    int
 
	sck			   *shardctrler.Clerk	//用于联系分片控制器的客户端
	Config 		   shardctrler.Config 	//需要更新的最新的配置
	LastConfig	   shardctrler.Config	//更新之前的配置，用于比对是否全部更新完成
}

type KvDataBase struct {
	KvData map[string]string 	//实际存放数据的map
	ConfigNum int				//这个分片是什么版本的，配置更新会进行修改(递增)
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
	//**************判断请求的分片服务器是否正确(如果更新配置，分片正在迁移就是这里解决，拒绝请求)
	///*********收到分片后就可以立即开始服务分片，不需要等待所有的分片到达
	shardId := key2shard(args.Key)
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.kvDataBase[shardId].KvData == nil {
		//*************分片还没有迁移完成(别人还没有发过来)
		reply.Err = ShardNotArrived
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
			reply.Value = replyMsg.Value
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(500 * time.Millisecond):
		//超时，返回结果，但是不更新Command -> Reply
		log.Printf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op.CommandType)
		reply.Err = ErrTimeout
	}
	//5、清除chan
	go kv.CloseChan(index)
}

func (kv *ShardKV) CloseChan(index int) {
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

	//判断请求的分片服务器是否正确
	shardId := key2shard(args.Key)
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.kvDataBase[shardId].KvData == nil {
		reply.Err = ShardNotArrived
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
	reply.Err = kv.startCommand(op)
}

//AddShard将分片从调用方移动到此服务器
func(kv *ShardKV) AddShard(args *SendShardArgs, reply *AddShardReply) {
	kv.mu.Lock()
	defer func() {
		//log.Printf("kvserver[%d]: 返回AddShard RPC请求,args=[%v];Reply=[%v]\n", kv.me, args.CommandId, reply.Err)
	}()
	//log.Printf("kvserver[%d]: 接收AddShard RPC请求,args=[%v]\n", kv.me, args.CommandId)
	//有不有都可以，这里只做一个判断处理，对于AddApplyCommand也是做了幂等性处理的(ConfigNum和kv.Config.Num)
	commandContext, ok := kv.clientReply[args.ClientId]
	if ok && commandContext.Command == args.CommandId {
		reply.Err = commandContext.Reply.Err
		kv.mu.Unlock()
		return
	} else if commandContext.Command > args.CommandId {
		//以前已经执行过了，所以这个请求时不合法的(客户端路由Leader的请求)，返回一个正常的使得客户端退出循环
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op {
		CommandType : "AddShard",
		ClientId : args.ClientId,
		CommandId : args.CommandId,
		ShardId : args.ShardId,	//分片id
		Shard : args.Shard,		//分片数据
		ClientReply : args.ClientReply, //接收者更新其状态(对该分片进行访问的重复表)
	}
	reply.Err = kv.startCommand(op)
}

//将共识操作提取出来
func (kv *ShardKV) startCommand(op Op) Err {
	index, term, isLeader := kv.rf.Start(op)
	//3、若不为Leader则直接返回Err
	if !isLeader {
		return ErrWrongLeader
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
			go kv.CloseChan(index)
			return replyMsg.Err
		} else {
			go kv.CloseChan(index)
			return ErrWrongLeader
		}
	case <- time.After(500 * time.Millisecond):
		//超时，返回结果，但是不更新Command -> Reply
		log.Printf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op.CommandType)
		go kv.CloseChan(index)
		return ErrTimeout
	}
	//5、清除chan
	//go kv.CloseChan(index)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) ReceiveApplyMsg(){
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

func (kv *ShardKV) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var commonReply ApplyNotifyMsg
	op := applyMsg.Command.(Op)
	commandContext, ok := kv.clientReply[op.ClientId]
	if op.CommandType == "Get" || op.CommandType == "Put" || op.CommandType == "Append" {
		//************当命令已经被应用过了：在这个地方就是关键的幂等性(论文6.3)(因为会出现Leader还没有回复客户端就宕机了)
		//************对于下面Leader对分片的共识操作是不需要的判断的
		if ok && commandContext.Command == op.CommandId {
			//log.Printf("kvserver[%d]: 该命令已被应用过,applyMsg: %v, commandContext: %v\n", kv.me, applyMsg, commandContext)
			commonReply = commandContext.Reply
			return
		}
		//************这里再进行判断是避免请求在共识的时候Config已经变化
		shardId := key2shard(op.Key)
		//**********如果符合最新的配置(分片是在当前Group副本组)
		if kv.Config.Shards[shardId] != kv.gid {
			commonReply.Err = ErrWrongGroup
			commonReply.Term = applyMsg.CommandTerm
		} else if kv.kvDataBase[shardId].KvData == nil {
			//如果应该存在的切片没有数据那么说明这个分片就还没到达(配置到达了，但是分片还没有到达)
			commonReply.Err = ShardNotArrived
			commonReply.Term = applyMsg.CommandTerm
		} else {
			//当命令未被应用过
			if op.CommandType == "Get" {
				value, ok := kv.kvDataBase[shardId].KvData[op.Key]
				if ok {
					//有数据时
					commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
				} else {
					//没有数据，value就等于""
					value = ""
					commonReply = ApplyNotifyMsg{ErrNoKey, value, applyMsg.CommandTerm}
				}
			} else if op.CommandType == "Put" {
				//往map里面插入或更新元素value
				kv.kvDataBase[shardId].KvData[op.Key] = op.Value
				commonReply = ApplyNotifyMsg{OK, op.Value, applyMsg.CommandTerm}
			} else if op.CommandType == "Append" {
				value, ok := kv.kvDataBase[shardId].KvData[op.Key]
				newValue := op.Value
				if ok {
					//若存在，直接append附加value
					newValue = value + op.Value
				}
				//如果不存在，则直接put进去
				kv.kvDataBase[shardId].KvData[op.Key] = newValue
				commonReply = ApplyNotifyMsg{OK, newValue, applyMsg.CommandTerm}
			}
		}
		if commonReply.Err != ErrWrongGroup && commonReply.Err != ShardNotArrived {
			//更新clientReply，用于去重(记录了每个client最后一次执行的信息)
			kv.clientReply[op.ClientId] = CommandContext{op.CommandId, commonReply}
		}
	} else {
		//Leader的共识(不需要返回reply)或者其他服务器发来的切片(AddShard)
		switch op.CommandType {
		case "UpConfig":	//配置更新
			//log.Printf("kserver[%d]: 执行了num %v -> %v", kv.me, kv.Config, op.UpConfig)
			kv.upConfigHandler(op)
		case "AddShard":	//增加切片
			commonReply.Term = applyMsg.CommandTerm
			//如果配置号比op的CommandId还低说明，说明当前不是最新配置(最新的配置信息还没有到，没有配置信息无法更新)，相当于判重
			if kv.Config.Num < op.CommandId {
				commonReply.Err = ConfigNotArrived
				break
			}
			kv.addShardHandler(op)
			commonReply.Err = OK
		case "RemoveShard":	//移除切片
			//删除操作来自以前的UpConfig
			kv.removeShardHandler(op)
		}
	}

	//通知handler（处理者）去响应请求
	replyCh, ok := kv.replyChMap[applyMsg.CommandIndex]
	if ok {
		replyCh <- commonReply
	}
	kv.lastApplied = applyMsg.CommandIndex
	//是否需要snapshot快照
	if kv.needSnapshot() {
		kv.startSnapshot(applyMsg.CommandIndex)
	}
}

//更新最新的config的handler
func (kv *ShardKV) upConfigHandler(op Op) {
	curConfig := kv.Config
	upConfig := op.UpConfig
	//过期的配置，这里的等于就相当于判重了(就可以省去自增的CommandId)
	if curConfig.Num >= upConfig.Num {
		return
	}
	//更新分片数据库的信息
	for shard, gid := range upConfig.Shards {
		//***************这里就处理新增的分片(而不是迁移的)
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			//如果更新的配置的gid与当前的配置的gid一样且分片为0(未分配)
			kv.kvDataBase[shard].KvData = make(map[string]string)
			kv.kvDataBase[shard].ConfigNum = upConfig.Num
		}
	}
	kv.LastConfig = curConfig
	kv.Config = upConfig
}

func (kv *ShardKV) addShardHandler(op Op) {
	//此分片已添加或是过时的命令(ConfigNum相当于重复表去重)
	if kv.kvDataBase[op.ShardId].KvData != nil || op.Shard.ConfigNum < kv.Config.Num {
		return
	}
	kv.kvDataBase[op.ShardId] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.KvData)
	for clientId, CommandContext := range op.ClientReply {
		r, ok := kv.clientReply[clientId]
		if !ok || r.Command < CommandContext.Command {
			kv.clientReply[clientId] = CommandContext
		}
	}
}

//删除迁移出去的分片
func (kv *ShardKV) removeShardHandler(op Op) {
	if op.CommandId < kv.Config.Num {
		return
	}
	//***************只有属于自己的分片才会有数据，不属于自己的就是nil
	kv.kvDataBase[op.ShardId].KvData = nil
	kv.kvDataBase[op.ShardId].ConfigNum = op.CommandId
}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	var proportion float32
	proportion = float32(kv.rf.GetRaftStateSize() / kv.maxraftstate)
	return proportion > 0.9
}

//主动开始snapshot(由leader在maxRaftState不为-1,而且目前接近阈值的时候调用)
func (kv *ShardKV) startSnapshot(index int) {
	//log.Printf("kvserver[%d]: 容量接近阈值,进行快照,rateStateSize=[%d],maxRaftState=[%d]\n", kv.me, kv.rf.GetRaftStateSize(), kv.maxraftstate)
	snapshot := kv.createSnapshot()
	//log.Printf("kvserver[%d]: 完成service层快照\n", kv.me)
	//通知Raft进行快照
	go kv.rf.Snapshot(index, snapshot)
}

//生成server的状态的snapshot，生成持久化的数据byte数组(序列化)，这里不要持久化
func (kv *ShardKV) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码kv数据(***其实这个就是Raft的log压缩后的样子)
	e.Encode(kv.kvDataBase)
	//编码clientReply(为了去重)
	e.Encode(kv.clientReply)
	e.Encode(kv.Config)
	e.Encode(kv.LastConfig)
	snapshotData := w.Bytes()
	return snapshotData
}

//被动应用snapshot
func (kv *ShardKV) ApplySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("kvserver[%d]: 接收到leader的快照\n", kv.me)
	if msg.SnapshotIndex < kv.lastApplied {
		//log.Printf("kvserver[%d]: 接收到旧的日志,snapshotIndex=[%d],状态机的lastApplied=[%d]\n", kv.me, msg.SnapshotIndex, kv.lastApplied)
		return
	}
	//判断此时有没有竞争
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		//只有日志落后并且需要的日志Leader已经Snapshot了才会发送snapshot
		kv.lastApplied = msg.SnapshotIndex
		//将快照中的service层数据进行加载
		kv.readSnapshot(msg.Snapshot)
		//log.Printf("kvserver[%d]: 完成service层快照\n", kv.me)
	}
}

//反序列化byte数组(发送的snapshot) -> 内存
func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvDataBase []KvDataBase
	var clientReply map[int64]CommandContext
	var Config, LastConfig shardctrler.Config
	if d.Decode(&kvDataBase) != nil || d.Decode(&clientReply) != nil || d.Decode(&Config) != nil || d.Decode(&LastConfig) != nil {
		log.Printf("kvserver[%d]: decode error\n", kv.me)
	} else {
		//**********之前的数据就直接覆盖了，kvDataBase和clientReply在反序列化就生成处理了，这里只需要引用指向即可
		kv.kvDataBase = kvDataBase
		kv.clientReply = clientReply
		kv.Config = Config
		kv.LastConfig = LastConfig
	}
}

//配置检测
//更新配置后发现有配置更新：
//1、收到别的group的切片后进行AddShards同步更新组内的切片
//2、成功发送不属于自己的切片或者超时则进行Gc
//您需要让您的服务器监视配置更改，并在检测到配置更改时启动分片迁移过程
//如果一个副本组丢失了一个分片，它必须立即停止向该分片中的键提供请求，并开始将该分片的数据迁移到接管所有权的副本组
//如果一个副本组获得一个分片，它需要等待让前任拥有者在接受对该分片的请求之前发送旧的分片数据
func (kv *ShardKV) ConfigDetectedLoop() {
	kv.mu.Lock()
	//一开始就是nil
	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()
	for !kv.killed() {
		//只有领导者需要处理配置任务
		if _, isLeader := rf.GetState(); !isLeader {
			//轮询配置周期
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()
		//***********只有当前配置完成了，才可以去轮询下一个新的配置
		//1、判断是否把不属于自己的部分分给别人了(拿着之前的Config和最新的Config进行比较即可，allSent()有一个不匹配就进行分片更新)
		//*************判断是否处于迁移期
		if !kv.allSent() {
			//对该分片进行访问的重复表(便于接受者判重)
			SepMap := make(map[int64]CommandContext)
			for k, v := range kv.clientReply {
				SepMap[k] = v
			}
			//拿着之前的Config和最新的Config进行比较即可
			for shardId, gid := range kv.LastConfig.Shards {
				//*********将最新配置里不属于自己的分片分给别人
				if gid == kv.gid && kv.Config.Shards[shardId] != kv.gid && kv.kvDataBase[shardId].ConfigNum < kv.Config.Num {
					if kv.Config.Shards[shardId] != 0 {
						//创建一个新的KvData，更新了Config.Num，发送分片数据(迁移操作)
						sendDate := kv.cloneShard(kv.Config.Num, kv.kvDataBase[shardId].KvData)
						//这里同样是会涉及到一个去重的问题(server之间)，相比客户端RPC通过在client端进行commandId自增，关于的配置的自增
						//只要利用配置号进行就可以，只要配置更新，那么一系列的操作就都会与最新的配置号有关
						args := SendShardArgs {
							ClientReply : SepMap,
							ShardId : shardId,
							Shard : sendDate,
							ClientId : int64(gid),
							CommandId : kv.Config.Num,
						}

						//shardId -> gid -> server names(用来RPC)
						serversList := kv.Config.Groups[kv.Config.Shards[shardId]]
						servers := make([]*labrpc.ClientEnd, len(serversList))
						for i,name := range serversList {
							servers[i] = kv.make_end(name)
						}
						
						//开启协程发送切片(自身的共识组也需要raft进行状态修改）
						go func(servers []*labrpc.ClientEnd, args *SendShardArgs) {
							index := 0
							//start := time.Now()
							for {
								var reply AddShardReply
								//对其他组的server发送RPC(需要发送给Leader)
								ok := servers[index].Call("ShardKV.AddShard", args, &reply)
								//如果给予切片成功，或者时间超时，这两种情况都需要进行GC掉不属于自己的切片(共识)
								//***********高configNum的group，应该会堵塞在需要发送数据的状态(ApplyCommand()中低配置的不会接收，因为自己没有)
								//如果配置号比op的CommandId还低说明，说明当前不是最新配置(最新的配置信息还没有到，没有配置信息无法更新)，相当于判重
								//导致其没有继续更新newConfig，低configNum的group发送数据，高配置不会接收(addShardHandler()中认为是过期的配置)
								//只要返回一个OK即可，其他 group 即使升级到更高的 config，也可能需要等待 configNum 低的 group 完成分片任务
								//整个系统才会滚动向前并整体可用
								//(Push和Pull)再者，如果低configNum宕机了，这个就是Add发送超时，那么如果低configNum恢复过来了
								//怎么让当前继续发送Add给低配置configNum呢(push模式)
								//所以我们这里规定只有别人确认收到了(返回OK)才能删除，只有自己全部发出去了才能轮询下一个新的配置
								//所以哪些低配置configNum就不会需要别人发送Add
								//所以如果低configNum没有恢复(新的Leader会产生)，整个系统是不可能滚动向前的并整体可用了
								//log.Printf("kvserver[%d]: 开始发送AddShard RPC;index=[%v];num=[%v]\n", kv.me, index, kv.Config.Num)
								// || time.Now().Sub(start) >= 2*time.Second，如果设置超时就会出现上面的情况高和低configNum的情况
								if ok && reply.Err == OK {
									//如果成功
									//log.Printf("kvserver[%d]: 成功了返回 %v", kv.me, reply.Err)
									kv.mu.Lock()
									op := Op {
										CommandType : "RemoveShard",
										ClientId : int64(kv.gid),
										CommandId : kv.Config.Num,
										ShardId : args.ShardId,
									}
									kv.mu.Unlock()
									//发送到Raft，自己进行共识，不需要返回reply(回复别人)，可能发送很多次(会产生会多次的共识)
									kv.startCommand(op)
									break
								}
								//log.Printf("kvserver[%d]: Add返回 %v", kv.me, reply.Err)
								//因为发送server节点可能不是Leader
								index = (index + 1) % len(servers)
								if index == 0 {
									break
								}
							}
						}(servers, &args)
					} else {
						//无效组，不需要发送，直接删除(相当于停用分片，感觉应该不会有这种傻逼操作吧)
						op := Op {
							CommandType : "RemoveShard",
							ClientId : int64(kv.gid),
							CommandId : kv.Config.Num,
							ShardId : shardId,
						}
						//发送到Raft，自己进行共识，不需要返回reply(回复别人)，可能发送很多次(会产生会多次的共识)
						kv.startCommand(op)
					}
				}
			}
			kv.mu.Unlock()
			//*******************傻逼操作(被我称为傻逼操作的代码，竟然是debug多天后通过测试的关键代码)
			// {10 [101 102 101 101 101 102 101 102 102 102] 
			// map[101:[server-101-0 server-101-1 server-101-2] 102:[server-102-0 server-102-1 server-102-2]]}
			// -> {11 [101 101 101 101 101 101 101 101 101 101] map[101:[server-101-0 server-101-1 server-101-2]]}
			//全部发送完一遍了，如果还没有成功就等一会(等一会也可以让Raft能稳定选出一个Leader)，如果不等待继续发送
			//可能会导致大量请求全部涌入101，并且raft共识层也会有大量的提交日志的请求(start()方法，占用了raft的锁)，就会导致Add共识的log超时
			//time.Sleep(UpConfigLoopInterval)
			time.Sleep(500 * time.Millisecond)
			//**************只有把不属于自己的分片分给别人了(成功了)才会执行下面的2
			continue
		}

		//2、判断别人发过来的分片是否都收到了
		//*************判断是否处于迁移期
		if !kv.allReceived() {
			kv.mu.Unlock()
			//傻逼操作
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		//********当前配置已配置，轮询下一配置
		curConfig = kv.Config
		//先把变量取出来，就可以释放锁，不占用锁
		sck := kv.sck
		kv.mu.Unlock()
		//**********
		//log.Printf("kvserver[%d]: 轮询配置ConfigNum = %d", kv.me, curConfig.Num + 1)
		newConfig := sck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num + 1 {
			//让其他线程可以拿到锁
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		//log.Printf("kvserver[%d]: 轮询到了新配置ConfigNum = %d", kv.me, newConfig.Num)
		//轮询到了新的配置(去共识)
		//如果最新配置共识失败呢：新的Leader会选出来，会继续进行轮询最新的配置然后进行共识
		op := Op {
			CommandType : "UpConfig",
			ClientId : int64(kv.gid),
			CommandId : newConfig.Num,
			UpConfig : newConfig,
		}
		kv.startCommand(op)
	}
}

func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		//如果当前配置中分片中的信息不匹配，且持久化中(map数据库)的配置号更小
		//说明还未将新配置的不属于自己的分片发送出去(就是还没有共识删除(删除会更新ConfigNum))
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.kvDataBase[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

//判断别人发过来的分片是否都收到了
func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {
		//这里使用ConfigNum来判断收到了别人发过来的分片
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.kvDataBase[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

//相当于一个深拷贝分片(避免赋值时引用指向)
func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) KvDataBase {
	migrateShard := KvDataBase {
		KvData : make(map[string]string),
		ConfigNum : ConfigNum,
	}
	for k,v := range KvMap {
		migrateShard.KvData[k] = v
	}
	return migrateShard
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	//创建分片数据库
	kv.kvDataBase = make([]KvDataBase, shardctrler.NShards)
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.clientReply = make(map[int64]CommandContext)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	//初始化分片控制器的Client
	kv.sck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//从快照中读取数据(***********Server不进行持久化，所有的持久化操作都是由Raft层完成)
	kv.readSnapshot(kv.rf.GetSnapshot())

	go kv.ReceiveApplyMsg()
	go kv.ConfigDetectedLoop()

	return kv
}
