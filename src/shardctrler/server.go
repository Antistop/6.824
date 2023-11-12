package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import (
	"time"
	"sync/atomic"
)


//逻辑和lab3一样，都是Raft实现线性化，由于Config数据量较小并且产生日志也比较小，所以不用处理快照
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	configs []Config // indexed by config num 按配置编号编制索引，这个就相当于数据库了
	replyChMap     map[int]chan ApplyNotifyMsg 	//ApplyNotifyMsg是chan的类型，Leader回复给客户端的响应结果的map（日志Index,ApplyNotifyMsg）
	clientReply    map[int64]CommandContext   	//一个能记录每一个客户端最后一次操作序号和应用结果的map（clientId，CommandContext）
}

//可以表示Reply
type ApplyNotifyMsg struct {
	Err Err
	Config Config
	//该被应用的command的term,便于RPC handler判断是否为过期请求
	//之前为leader并且start了,但是后来没有成功commit就变成了follower,导致一开始Start()得到的index处的命令不一定是之前的那个,所以需要拒绝掉;
    //或者是处于少部分分区的leader接收到命令,后来恢复分区之后,index处的log可能换成了新的leader的commit的log了
	Term int
}

type CommandContext struct {
	Command int 			//该client目前的commandId
	Reply ApplyNotifyMsg 	//该command的响应
}

//Server传给raft的Start方法的命令结构Command interface{}
type Op struct {
	// Your data here.
	CommandType  string
	ClientId 	 int64
	CommandId	 int
	Num_Query    int
	Servers_Join map[int][]string
	Gids_Leave	 []int
	Shard_Move   int
	Gid_Move	 int
}

//添加新的Group副本组
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	commandContext, ok := sc.clientReply[args.ClientId]
	if ok && commandContext.Command == args.CommandId {
		reply.Err = commandContext.Reply.Err
		sc.mu.Unlock()
		return
	} else if commandContext.Command > args.CommandId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op {
		CommandType : "Join",
		Servers_Join : args.Servers,
		ClientId : args.ClientId,
		CommandId : args.CommandId,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	sc.mu.Lock()
	sc.replyChMap[index] = replyCh
	sc.mu.Unlock()
	select {
	case replyMsg := <- replyCh:
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go sc.CloseChan(index)
}

func (sc *ShardCtrler) CloseChan(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, ok := sc.replyChMap[index]
	if !ok {
		return
	}
	close(ch)
	delete(sc.replyChMap, index)
}

//删除副本组
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	commandContext, ok := sc.clientReply[args.ClientId]
	if ok && commandContext.Command == args.CommandId {
		reply.Err = commandContext.Reply.Err
		sc.mu.Unlock()
		return
	} else if commandContext.Command > args.CommandId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op {
		CommandType : "Leave",
		Gids_Leave : args.GIDs,
		ClientId : args.ClientId,
		CommandId : args.CommandId,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	sc.mu.Lock()
	sc.replyChMap[index] = replyCh
	sc.mu.Unlock()
	select {
	case replyMsg := <- replyCh:
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go sc.CloseChan(index)
}

//在副本组之间移动分片，为指定的分片分配指定的组
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	commandContext, ok := sc.clientReply[args.ClientId]
	if ok && commandContext.Command == args.CommandId {
		reply.Err = commandContext.Reply.Err
		sc.mu.Unlock()
		return
	} else if commandContext.Command > args.CommandId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op {
		CommandType : "Move",
		Shard_Move : args.Shard,
		Gid_Move : args.GID,
		ClientId : args.ClientId,
		CommandId : args.CommandId,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	sc.mu.Lock()
	sc.replyChMap[index] = replyCh
	sc.mu.Unlock()
	select {
	case replyMsg := <- replyCh:
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go sc.CloseChan(index)
}

//查询指定配置号的Config信息
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	commandContext, ok := sc.clientReply[args.ClientId]
	if ok && commandContext.Command == args.CommandId {
		reply.Err = commandContext.Reply.Err
		reply.Config = commandContext.Reply.Config
		sc.mu.Unlock()
		return
	} else if commandContext.Command > args.CommandId {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op {
		CommandType : "Query",
		Num_Query : args.Num,
		ClientId : args.ClientId,
		CommandId : args.CommandId,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	sc.mu.Lock()
	sc.replyChMap[index] = replyCh
	sc.mu.Unlock()
	select {
	case replyMsg := <- replyCh:
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
			reply.Config = replyMsg.Config
		} else {
			reply.Err = ErrWrongLeader
		}
	case <- time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go sc.CloseChan(index)
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}


func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) ReceiveApplyMsg() {
	for !sc.killed() {
		select {
		case msg := <- sc.applyCh:
			if msg.CommandValid {
				//合法日志条目就应用日志
				sc.ApplyCommand(msg)
			} else {
				//非法消息
			}
		}
	}
}

func (sc *ShardCtrler) ApplyCommand(applyMsg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	var commonReply ApplyNotifyMsg
	op := applyMsg.Command.(Op)
	commandContext, ok := sc.clientReply[op.ClientId]
	//************当命令已经被应用过了：在这个地方就是关键的幂等性(论文6.3)(因为会出现Leader还没有回复客户端就宕机了)
	if ok && commandContext.Command == op.CommandId {
		//log.Printf("kvserver[%d]: 该命令已被应用过,applyMsg: %v, commandContext: %v\n", sc.me, applyMsg, commandContext)
		commonReply = commandContext.Reply
		return
	}
	//当命令未被应用过
	if op.CommandType == "Join" {
		sc.configs = append(sc.configs, *sc.JoinHandler(op.Servers_Join))
		commonReply = ApplyNotifyMsg{OK, sc.configs[len(sc.configs) - 1], applyMsg.CommandTerm}
	} else if op.CommandType == "Leave" {
		sc.configs = append(sc.configs, *sc.LeaveHandler(op.Gids_Leave))
		commonReply = ApplyNotifyMsg{OK, sc.configs[len(sc.configs) - 1], applyMsg.CommandTerm}
	} else if op.CommandType == "Move" {
		sc.configs = append(sc.configs, *sc.MoveHandler(op.Gid_Move, op.Shard_Move))
		commonReply = ApplyNotifyMsg{OK, sc.configs[len(sc.configs) - 1], applyMsg.CommandTerm}
	} else if op.CommandType == "Query" {
		//如果该数字是-1或大于已知的最大配置数，则分片器应使用最新配置进行回复
		if op.Num_Query == -1 || op.Num_Query >= len(sc.configs) {
			commonReply = ApplyNotifyMsg{OK, sc.configs[len(sc.configs) - 1], applyMsg.CommandTerm}
		} else {
			commonReply = ApplyNotifyMsg{OK, sc.configs[op.Num_Query], applyMsg.CommandTerm}
		}
	}
	//通知handler（处理者）去响应请求
	replyCh, ok := sc.replyChMap[applyMsg.CommandIndex]
	if ok {
		replyCh <- commonReply
	}
	//更新clientReply，用于去重(记录了每个client最后一次执行的信息)
	sc.clientReply[op.ClientId] = CommandContext{op.CommandId, commonReply}
	//log.Printf("kvserver[%d]: 更新ClientId=[%d],CommandId=[%d]\n", sc.me, op.ClientId, op.CommandId)
}

//处理Join进来的gid，加入新的组后需要重新进行分片负载均衡
//实验：shardctrler应该通过创建一个包含新副本组的新配置来做出反应
//新配置应尽可能将分片均匀地分配到整组组中，并应移动尽可能少的分片以实现该目标
func (sc *ShardCtrler) JoinHandler(servers map[int][] string) *Config {
	//取出最新的配置config然后将分组加进去
	lastConfig := sc.configs[len(sc.configs) - 1]
	newGroups := make(map[int][]string)

	//遍历map，第一个参数为key(gid)，第二个参数为value(servers[])
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	//加入需要加入的新副本组，如果加入组gid之前配置有，那就是更新这个组gid对应的服务器servers(应该不存在这种傻逼配置吧)
	for gid, serverLists := range servers {
		newGroups[gid] = serverLists
	}

	//记录每个分组有几个分片(gid -> 分片数量)
	GroupMap := make(map[int]int)
	for gid := range newGroups {
		GroupMap[gid] = 0
	}
	//记录每个组里多少分片
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GroupMap[gid]++
		}
	}
	//都没有存自然不需要分片均衡，初始化阶段
	if len(GroupMap) == 0 {
		return &Config {
			Num : len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}
	//需要分片均衡的情况
	return &Config {
		Num : len(sc.configs),
		Shards : sc.loadBalance(GroupMap, lastConfig.Shards),
		Groups : newGroups,
	}
}

//删除副本组，也需要进行分片负载均衡
func (sc *ShardCtrler) LeaveHandler(gids []int) *Config {
	lastConfig := sc.configs[len(sc.configs) - 1]
	newGroups := make(map[int][]string)
	//取出最新配置的groups组进行填充
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	//删除对应的gid
	for _, leaveGid := range gids {
		delete(newGroups, leaveGid)
	}

	//记录每个分组有几个分片(gid -> 分片数量)
	GroupMap := make(map[int]int)
	//需要先处理删除了的gid组，然后再进行分片均衡
	newShard := lastConfig.Shards
	for gid := range newGroups {
		GroupMap[gid] = 0
	}
	//记录每个组里多少分片
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			//这里需要判断gid是否被删除
			if _, ok := newGroups[gid]; ok {
				GroupMap[gid]++
			} else {
				newShard[shard] = 0
			}
		}
	}
	//直接删没了
	if len(GroupMap) == 0 {
		return &Config {
			Num : len(sc.configs),
			Shards : [10]int{},
			Groups : newGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, newShard),
		Groups: newGroups,
	}
}

//在副本组之间移动分片，为指定的分片分配指定的组
func (sc *ShardCtrler) MoveHandler(gid int, shard int) *Config {
	lastConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{
		Num : len(sc.configs),
		Shards : [10]int{},
		Groups : map[int][]string{},
	}
	//填充并赋值
	for shards, gids := range lastConfig.Shards {
		newConfig.Shards[shards] = gids
	}
	//移动分片
	newConfig.Shards[shard] = gid

	for gids, servers := range lastConfig.Groups {
		newConfig.Groups[gids] = servers
	}
	return &newConfig
}


//分片均衡(NShards就是一个常量表示分片的数量10)
//即任意两个group之间的shard数目相差不能为1
func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GroupMap)
	ave := NShards / length
	remainder := NShards % length
	//排序，根据map中的value进行排序（使用第二种方法）
	//java中有两种：1、把map放入List中(List<Map>)，再Collections.sort排序，2、单独把Key拿出来成一个数组，然后根据key或value排序
	sortGids := sortGroupShard(GroupMap)

	//先把负载多的部分释放出来(从大到小)
	//这就是排序的目的，如果不排序直接遍历map，可能会先遍历出负载少的，但是这个时候不知道那个组负载高，所以没有分片可以分配
	for i := 0; i < length; i++ {
		target := ave
		//判断这个gid是否需要更多的分配，因为不可能完全均分，在前面remainder个的应该是ave+1
		//这里就是把前面rermainder个的少释放ave，后面的多释放一个ave+1
		if i >= length - remainder {
			target = ave + 1
		}
		//超出负载
		if GroupMap[sortGids[i]] > target {
			changeNum := GroupMap[sortGids[i]] - target
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				//先把负载多的释放出来(赋值0，因为gid从1开始)
				if gid == sortGids[i] {
					lastShards[shard] = 0
					changeNum--
				}
			}
			GroupMap[sortGids[i]] = target
		}
	}
	//把多出来的分片分配给为负载少的group
	for i := 0; i < length; i++ {
		target := ave
		//前面remainder个多分配一个
		if i >= length - remainder {
			target = ave + 1
		}
		if GroupMap[sortGids[i]] < target {
			changeNum := target - GroupMap[sortGids[i]]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				//先把负载多的释放出来(赋值0)
				if gid == 0 {
					lastShards[shard] = sortGids[i]
					changeNum--
				}
			}
			GroupMap[sortGids[i]] = target
		}
	}
	return lastShards
}

//返回一个根据value排序好的数组(降序)
func sortGroupShard(GroupMap map[int]int) []int {
	length := len(GroupMap)
	gidSlice := make([]int, 0, length)
	//将key单独拿出来成一个数组
	for gid, _ := range GroupMap {
		gidSlice = append(gidSlice, gid)
	}
	//把负载压力大的排前面，不清楚go的API，直接写个冒泡
	for i := 0; i < length - 1; i++ {
		for j := length - 1; j > i; j-- {
			//如果频数相等，就按gid小的排前面
			if GroupMap[gidSlice[j]] < GroupMap[gidSlice[j-1]] || 
			(GroupMap[gidSlice[j]] == GroupMap[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	//创建了一个初始长度为1的Config切片(动态数组)
	//configs[0]表示边界条件，无效
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	//创建Raft（也就是启动raft）
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.replyChMap = make(map[int]chan ApplyNotifyMsg)
	sc.clientReply = make(map[int64]CommandContext)

	go sc.ReceiveApplyMsg()

	return sc
}
