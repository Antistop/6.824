package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

//https://pdos.csail.mit.edu/6.824/  6.824课程官网

//https://pdos.csail.mit.edu/6.824/notes/raft_diagram.pdf  Raft交互图
//https://blog.csdn.net/darkLight2017/article/details/122570027  博客（实现与踩坑）
//https://thesquareplanet.com/blog/students-guide-to-raft/  Raft学生指南
//https://www.cnblogs.com/lizhaolong/p/16437274.html 2A
//https://www.cnblogs.com/lizhaolong/p/16437273.html 2B
//https://www.cnblogs.com/lizhaolong/p/16437272.html 2C
//https://github.com/Super-long/MIT-6.824/blob/master/raft/raft.go 上面三个的全部代码
//https://github.com/s09g/raft-go 2A、2B、2C  B站视频：https://www.bilibili.com/video/BV1CQ4y1r7qf/ s09g谷歌摸鱼工程师
//https://www.cnblogs.com/pxlsdz/p/15557635.html 2A、2B、2C、2D
//https://blog.csdn.net/weixin_45938441/article/details/125179308 2D 博客（有源码）
//https://blog.csdn.net/qq_40443651/article/details/116091524 2D 博客（有源码）


import (
	"bytes"
	"sync"
	"sync/atomic"
	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
	"log"
)

//本质：保证所有机器上的日志最终一致，允许一组机器像一个整体一样工作，即使其中一些机器出现故障也能够继续工作下去
//共识：保证所有机器相同index上的log一致
//细节：Leader选举和日志复制

//读写分离、心跳发送形式(流水线)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int //lab3：该log的term，以便上层应用可以根据term判断是否过期(index处期望的term和该term不同的情况)

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//*********全局索引：是相对历史上全部的 log 而言的，是从0开始到现在，每个 log 里面都记录着一个独一无二的顺序索引，这就是全局索引
//为了保证索引的统一，在节点与节点之间，节点与 service 之间传递的那些索引，都应该是全局索引，另外 log 内部记录的索引也是全局索引log.Index
//matchIndex，commitIndex，lastApplied，CommitIndex等索引都是全局索引
//*********局部索引：是相对于当前节点保存的 log 而言的，从0到 len(rf.log) - 1，在节点内部使用的索引，都应该是局部索引
//因为 snapshot 会丢弃掉历史 log，所以使用全局索引是会导致数组越界的，所以需要转换成局部索引
//*********全局索引转换为局部索引非常简单：全局索引 - 当前结点的 lastSnapshotIndex = 局部索引
//******************但是在没有snapshot之前，log.Index和数组下标都是一一对应的
//为什么matchIndex，commitIndex，lastApplied，CommitIndex这些不使用局部索引呢，因为snapshot之后就算使用局部索引也需要改变
//还不如当全局索引使用更容易理解
type Entry struct {
	Command interface{}  //命令
	Term    int
	Index 	int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 所有对等方peers的RPC端点，相当于所有的Raft实例的全局唯一ID集合
	persister *Persister          // Object to hold this peer's persisted state 用于保持此对等方peers的持久状态的对象
	me        int                 // this peer's index into peers[]  peers数组中自己对等点的索引，相当于当前节点id
	dead      int32               // set by Kill() 是否死亡，1表示死亡，0表示还活着
	//上面的都是RPC的一些相关参数

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//请参阅本文(Raft论文)的图2，了解Raft服务器必须维护的状态

	//2A
	current_state   string     	 	//当前状态
	currentTerm 	int     		//服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增）
	votedFor 		int     		//当前任期内收到选票的候选者id 如果没有投给任何候选者则为空
	electionTime    time.Time 		//选举超时时间
	heartbeatTime   time.Duration  	//心跳超时时间
	
	//2B
	log	 []Entry					//日志条目;每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）
	commitIndex int  				//已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int  				//已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	
	nextIndex []int   				//对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
	matchIndex []int 				//对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	applyCh chan ApplyMsg 			//该服务希望您的实现为每个新提交的日志条目发送一个ApplyMsg到Make()的applyCh通道参数
	applyCond *sync.Cond  		    //用于在提交新条目后唤醒applier goroutine，和Java的wait和notify一样
}

// return currentTerm and whether this server
// believes it is the leader.
//return currentTerm以及此服务器是否认为它是领导者
//就是测试2A的判断当前是否只有一个Leader，具体实现取决于结构体如何设计
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.current_state == "LEADER"
	return term, isleader
}

//lab3B：GetRaftStateSize获取当前raft状态的大小
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//lab3B：获取到持久化的snapshot
func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
//将Raft的持久状态保存到稳定的存储中，在崩溃并重新启动后，可以在那里检索到它。关于什么应该是持久性的描述，请参见论文的图2
//如同Java的序列化
//*********我们的任务就是在所有改变了上面三个变量的地方调用rf.persist()就可以了
//使用labgob编码器；请参阅persist()和readPersist()中的注释。 labgob类似于 Go 的gob编码器
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//持久化三个参数论文图2，只要我们持久化这三个参数我们就可以通过做到在节点重启后做到重新复盘宕机前的状况
	//log：如果当前服务器的log条目，是使得领导者提交log条目的多数Follower之一，则需要在重新启动后必须记住该条目
	//这样下一个选举出来的领导者才会包括该条目，所以选举限制确保新的领导者也有该条目
  	//votedFor：防止客户投票给一位候选人，然后重新启动，然后在同一任期内投票给不同的候选人，可能导致同一任期的两位领导人
  	//currentTerm：避免追随一个被取代的领导，然后在被取代的选举中投票，每个Term里面每个节点只能投一票，这保证了算法的正确性
	//e.Encode(rf.currentTerm)
	//e.Encode(rf.votedFor)
	//e.Encode(rf.log)

	//为什么要保存这3个变量，commitIndex, lastApplied, next/matchIndex[]为什么不保存，MIT课程已经提到了
	//为什么不保存，持久性通常是性能的瓶颈，硬盘写入需要 10 毫秒，SSD 写入需要 0.1 毫秒，因此持久性将我们限制在 100 到 10,000 次操作/秒
	//（另一个潜在瓶颈是 RPC，在 LAN（局域网） 上需要 << 1 毫秒）
	//应对持久性缓慢的技巧：每次磁盘写入时批处理多个新日志条目，持久保存到电池支持的RAM，而不是磁盘
	//这样是惰性处理并有丢失最后几个提交更新的风险（批处理而不是sync同步写入磁盘）
	//Raft 的 RPCs 需要接收方将信息持久化的保存到稳定存储中去，所以广播时间大约是 0.5 毫秒到 20 毫秒，取决于存储的技术（论文5.6）
	//****通俗的说就是持久化这么多属性会影响性能（RPC、磁盘、SSD性能不允许），不是必要的就尽量不持久化
	//服务（例如 k/v 服务器）如​​何恢复其状态崩溃+重启后
	//简单的方法：从空状态开始，重新播放 Raft 的整个持久化日志，lastApplied从零开始，因此您可能不需要额外的代码！这就是图 2 所做的
	//但是对于长期运行的系统来说，重放速度太慢了：所以可以使用 Raft 快照并仅重放没有进行快照的日志，这个方法更快，所以前面的属性也就不用持久化了
	//data := w.Bytes()
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

//生成持久化的数据byte数组
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.log[0].Index)
	e.Encode(rf.log[0].Term)
	//非必要，但是便于Leader处理Follower的响应日志请求（判断复制是否过半来增加rf.commitIndex）
	//持久化就避免了每次crash宕机，commitIndex为0，导致减去快照index，log[commitIndex+1 - rf.log[0].Index]越界
	//大论文3.8提到了：状态机可以是易失性的，也可以是持久性的
	//重新启动后，必须通过重新应用日志条目（应用最新快照后；请参阅第 5 章）来恢复易失性状态机
	//持久状态机在重启后已经应用了大多数条目。为了避免重新应用它们，其最后应用的索引(commitIndex)也必须是持久的
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
//恢复以前的持久状态，如同Java的反序列化
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	var commitIndex int 

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil ||
			d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil || d.Decode(&commitIndex) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.log[0].Index = lastIncludedIndex
		rf.log[0].Term = lastIncludedTerm
		rf.commitIndex = commitIndex
	}
}

//快照rpc
//实验说了：在单个InstallSnapshot RPC中发送整个快照。不要实现图 13 的偏移机制来拆分快照
type InstallSnapshotArgs struct {
	Term 				int
	LeaderId 			int		//领导人的 Id，以便于跟随者重定向请求
	LastIncludedIndex	int		//快照中包含的最后日志条目的索引值
	LastIncludedTerm	int		//快照中包含的最后日志条目的任期号
	//Offset				int		//分块在快照中的字节偏移量
	Data 				[]byte	//从偏移量开始的快照分块的原始字节
	//Done 				bool	//如果这是最后一个分块则为 true
}

type InstallSnapshotReply struct {
	Term int	//当前任期号（currentTerm），便于领导人更新自己
}

//向指定节点发送快照
func (rf *Raft) sendSnapshotToPeer(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs {
		Term : rf.currentTerm,
		LeaderId : rf.me,
		LastIncludedIndex : rf.log[0].Index,
		LastIncludedTerm : rf.log[0].Term,
		Data : rf.GetSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.sendSnapshot(server, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.current_state != "LEADER" || rf.currentTerm != args.Term {
			return
		}
		//返回Term比自己大（网络分区）
		if reply.Term > rf.currentTerm {
			rf.current_state = "FOLLOWER"
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}

		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		return
	}
}


//Follower接收Leader日志快照处理
//***********leader 发送来的 InstallSnapshot：领导者发送快照RPC请求给追随者
//当raft收到其他节点的压缩请求后，就会生成一个快照命令并发送到applyChan来进行处理
//然后把请求上报给上层应用，然后上层应用调用rf.CondInstallSnapshot()来决定是否应用该快照
//********快照命令并不算做日志的一项，该命令和日志所记录的命令是分开进行处理的
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.current_state = "FOLLOWER"
	rf.resetElectionTimer()

	//过时的快照
	//如果该 snapshot 的 LastIncludedIndex 小于等于Follower 的 commitIndex，那说明本地已经包含了该 snapshot 所有的数据信息
	//尽管可能状态机还没有这个 snapshot 新，即 lastApplied 还没更新到 commitIndex，但是 applier 协程也一定尝试在 apply 了
	//此时便没必要再去用 snapshot 更换状态机了
	if args.LastIncludedIndex <= rf.log[0].Index {
		//log.Printf("过时的快照: LastIncludedIndex %d index %d", args.LastIncludedIndex, rf.log[0].Index)
		rf.mu.Unlock()
		return
	}

	//将快照后的log切割，快照前的直接applied
	//不可以直接rf.log = rf.log[index - rf.log[0].Index:]会有bug
	index := args.LastIncludedIndex

	tempLog := make([]Entry, 0)
	//加一个空Entry，作为快照的截止日志的标志
	tempLog = append(tempLog, Entry{})

	//把快照后的log切割
	for i := index + 1; i <= rf.log[len(rf.log) - 1].Index; i++ {
		tempLog = append(tempLog, rf.log[i - rf.log[0].Index])
	}

	rf.log = tempLog
	
	rf.log[0].Term = args.LastIncludedTerm
	rf.log[0].Index = args.LastIncludedIndex

	//apply申请了快照就应该重置commitIndex、lastApplied
	//我认为这个也不需要？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)

	//接收发来的快照，并提交一个命令处理
	//对于更新的 snapshot，这里通过异步的方式将其 push 到 applyCh 中
	msg := ApplyMsg {
		SnapshotValid : true,
		Snapshot : args.Data,
		SnapshotTerm : args.LastIncludedTerm,
		SnapshotIndex : args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
	//log.Printf("lastLog.Index %d len %d", rf.log[len(rf.log) - 1].Index, len(rf.log))
	//log.Printf("[FollowerInstallSnapShot]server %d installsnapshot from leader %d, index %d",rf.me,args.LeaderId,args.LastIncludedIndex)
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
//一个上层服务想要切换到快照，只有当Raft在applyCh上传送快照后没有最新信息时，才可以这样做
//*****对于这个函数的背景其实就是你发送了快照，那么你发送的快照就要上传到applyCh，而同时你的appendEntries也需要进行上传日志，可能会导致冲突
//之前，本实验建议您实现一个名为CondInstallSnapshot的函数，以避免需要协调在applyCh上发送的快照和日志条目，可实际上
//只要你在applied的时候做好同步，加上互斥锁。那么就可以避免这个问题，因此实验室中也提到这个api已经是废弃的，不鼓励去实现，简单的返回一个true就行
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//该服务表示它已经创建了一个快照，其中包含所有信息，包括索引，这意味着服务不再需要该索引(参数index)之前的日志，**Raft现在应该尽可能地修剪它的日志
//index int，这个参数是快照的截止index，snapshot []byte，上层service传来的快照字节流，将会包含所有截止到index的信息
//********服务端触发的日志压缩：上层应用发送快照数据给Raft实例，Raft应该在该点index之前丢弃它的日志条目
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.log[0].Index
	//如果当前快照包含的最后一条log的index ≥ 参数中快照的index，则没必要进行压缩
	//********论文里面说了：每个服务器独立的创建快照，只包括已经被提交的日志
	if index <= snapshotIndex || index > rf.commitIndex || index > rf.log[len(rf.log) - 1].Index {
		return
	}
	//删掉index前的所有日志
	rf.log = rf.log[index - snapshotIndex:]
	//0位置就是快照的index，把Command置为nil可能会影响测试出现apply error（不同节点同一index位置不同，一个为null就是这里设置的）
	//rf.log[0].Command = nil

	//apply申请了快照就应该重置commitIndex、lastApplied
	//？？？？？？？？？？？？？？？？？？？？？我认为可以不用重置commitIndex（和上面的if判断冲突了）
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	//实验persister.go里面已经帮我们写好方法了
	//如果服务器崩溃，它必须从持久数据重新启动。您的 Raft 应该同时保留 Raft 状态和相应的快照(便于向日志落后的Follower发送快照)
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	//log.Printf("commmd  %d", rf.log[0].Command)
	//log.Printf("Node %d Snapshot %d log[0].Index %d", rf.me, index, rf.log[0].Index)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//用于投票RPC通信的结构体：由候选人负责调用用来征集选票
//一定要大写开头，不然RPC通信过程序列化/反序列化的时候可能找不到
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//请求投票 RPC：由候选人负责调用用来征集选票
	Term int 			//候选人的任期号
	CandidateId int 	//请求选票的候选人的 Id
	LastLogIndex int 	//候选人的最后日志条目的索引值
	LastLogTerm int 	//候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int 			//当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool 	//候选人赢得了此张选票时为真
}

//用于日志RPC通信的结构体
//根据论文，被领导者调用用于日志条目的复制同时也被当做心跳使用
type AppendEntryArgs struct {
	Term int 			//领导者的任期
	LeaderId int 		//领导者ID，因此跟随者可以对客户端进行重定向
				 		//（译者注：跟随者根据领导者id把客户端的请求重定向到领导者，比如有时客户端把请求发给了跟随者而不是领导者）
	PrevLogIndex int 	//紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm int		//紧邻新日志条目之前的那个日志条目的任期
	Entries  []Entry	//需要被保存的日志条目（被当做心跳使用是日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int 	//领导者的已知已提交的最高的日志条目的索引
}

//XIndex：算法可以通过减少被拒绝（冲突）的附加日志 RPCs 的次数来优化。例如，当附加日志 RPC 的请求被拒绝的时候
//跟随者可以返回包含冲突的条目的任期号和自己存储的那个任期的最早的索引地址(论文5.3)
//借助这些信息，领导人可以减小 nextIndex 越过所有*******那个任期冲突的所有日志条目；这样就变成每个任期需要一次附加条目 RPC 而不是每个条目一次
type AppendEntryReply struct {
	Term int 			//当前任期，对于领导者而言它会更新自己的任期（因为leader可能会出现分区）
	Success bool 		//结果为真 如果跟随者所含有的条目和prevLogIndex以及prevLogTerm匹配上了
	XIndex int  		//用于返回与leader日志冲突项的Index
	XTerm  int			//冲突Term
	XLen int 			//冲突的长度，我们这里使用了XIndex来代替
	//作者提到了3个字段的优化
}

//Leader向Follower发送心跳(这一个函数就包括了两种形式的心跳，一种有附加日志，一种没有)超时时间为heartbeatTimer
func (rf *Raft) SendAppendEntriesToAllFollower(heartbeat bool) {
	lastLog := rf.log[len(rf.log) - 1]
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			//为什么Leader也需要重置定时器，我的理解是：避免分区过后，收到其他Leader的心跳，如果Leader没有重置定时器的话
			//ticker()中就会立马产生LeaderElection
			rf.resetElectionTimer()
			continue
		}

		//installSnapshot，如果rf.nextIndex[i]-1小于等lastIncludeIndex,说明followers的日志小于自身的快照状态，将自己的快照发过去
		//同时要注意的是比快照还小时，已经算是比较落后
		if rf.nextIndex[i] <= rf.log[0].Index {
			go rf.sendSnapshotToPeer(i)
			continue
		}

		//**************** rules for leader 3
		//1、如果对于一个跟随者，最后日志条目的索引值大于等于 nextIndex，那么发送从 nextIndex 开始的所有日志条目，或者发送心跳
		//******刚成为leader的时候更新过，所以第一次entry为空，发送空的日志，但是其他的属性还是可以同步传一下
		//******就可以通过PrevLogIndex来计算出nextIndex和matchIndex的真实情况
		//rf.nextIndex[i] = rf.log[len(rf.log) - 1].Index + 1，rf.matchIndex[i] = 0为成为领导人的设置
		//2、当我们在Start()中加入一条新日志的时候这里会在心跳包中发送出去
		if lastLog.Index >= rf.nextIndex[i] || heartbeat {
			nextIndex := rf.nextIndex[i]
			//边界处理
			// if nextIndex <= rf.log[0].Index {
			// 	nextIndex = rf.log[0].Index + 1
			// }
			//说明发送的是心跳，不需要发送日志过去
			if lastLog.Index < nextIndex {
				nextIndex = lastLog.Index + 1
			}
			//如果snapshot，还要减去当前结点的 lastSnapshotIndex（log.Index和log数组的下标是对应的）
			prevLog := rf.log[nextIndex - rf.log[0].Index - 1]
			args := AppendEntryArgs {
				Term : rf.currentTerm,
				LeaderId : rf.me,
				PrevLogIndex : prevLog.Index,	//紧邻新日志条目之前的那个日志条目的索引
				PrevLogTerm : prevLog.Term,
				LeaderCommit : rf.commitIndex,
			}

			//批量发送，全部发过去
			args.Entries = rf.log[nextIndex - rf.log[0].Index:]

			//log.Printf("心跳{Node %v} to {Node %v} Term %v PrevLogIndex %v PrevLogTerm %v LeaderCommit %v", rf.me, i, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

			//并行效率更高，发起投票需要异步进行，从而也不阻塞当前线程
			go func(serverNumber int, args AppendEntryArgs, rf *Raft) {
				reply := AppendEntryReply{}
				//******直到所有的跟随者都最终存储了所有的日志条目，在领导人将创建的日志条目复制到大多数的服务器上的时候，日志条目就会被提交
				ok := rf.sendAppendEntries(serverNumber, &args, &reply)
				if ok {
					//处理接收结果
					rf.handleAppendEntries(serverNumber, &args, &reply)
				}
			}(i, args, rf)
		}
	}
}

//Follwer接收Leader日志同步处理（还是一样两种形式的心跳）
/*
 * 1.判断当前Term和leaderTerm的大小，前者大于后者的话拒绝，小于的话改变节点状态
 * 2.进行一个冲突判断，而Follwer节点的日志数目(Index) 小于或者大于等于 args.PrevLogIndex情况：***论文[5.3]
*/
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//更换Leader后，任期会比之前Leader大，所以之前Leader传过来的就会拒绝没用了
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//排除上面一个不是正确的Leader心跳，重置选举超时时间（收到心跳）
	rf.resetElectionTimer()
	
	//Leader选举成功，或者分区，我们下一次RPC心跳再进行日志同步
	if args.Term > rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.current_state = "FOLLOWER" 	//Term落后，或者发送心跳需要更改当前状态
		rf.currentTerm = args.Term 		//落后于leader的时候更新Term
		rf.votedFor = -1				//更新voteFor，否则在下一轮选举中可能出现两个Leader，在选举成功的时候初始化每一个Follower也可以
		rf.persist()
		return
	}

	//新选举出来的Leader发送心跳
	if rf.current_state == "CANDIDATE" {
		rf.current_state = "FOLLOWER"
	}

	//***********其实只有Term等于args.Term同一任期才会同步日志，可以判断日志是否冲突来返回reply.XIndex的值
	
	//************前面的日志没到，后面的日志到了没用，因为后面的日志发送的PrevLogIndex对不上（这里就说明了）
	if rf.log[len(rf.log) - 1].Index < args.PrevLogIndex {	//此节点日志小于Leader
		//**************但是不能确定是否有冲突，把它放到下一次RPC去判断
		reply.XIndex = rf.log[len(rf.log) - 1].Index + 1
		reply.XTerm = -1
		reply.Success = false
		return
	}

	prevIndex := args.PrevLogIndex - rf.log[0].Index

	//要插入的前一个index小于快照index，几乎不会发生
	if prevIndex < 0 {
		reply.Term = rf.currentTerm
		reply.XIndex = rf.log[0].Index + 1
		reply.XTerm = -1
		reply.Success = false
		return
	}

	//此节点日志大于等于Leader并且在相同index上日志不同（判断是否有冲突）
	//有冲突，此时要使leader减少PrevLogIndex来寻找各个节点相同的日志
	if rf.log[prevIndex].Term != args.PrevLogTerm {
		reply.XIndex = args.PrevLogIndex //多出的日志一定会被舍弃掉要和leader同步

		//********往前找出与reply.XIndex任期相同的日志（论文5.3），因为这些可能也是冲突的日志，之前为Leader这一任期未提交的日志
		//不太可能要找到具体和Leader匹配的日志，因为不确定是哪一个，不可能把Leader的日志全部传过来再进行比较吧
		//只能尽量的去找，Raft学生指南：令XTerm = log[prevLogIndex].Term，XIndex = log中第一条term = XTerm的Index
		//跟随者可以返回包含冲突的条目的任期号和自己存储的那个任期的最早的索引地址(论文5.3)
		//借助这些信息，领导人可以减小 nextIndex 越过所有*******那个任期冲突的所有日志条目
		T := rf.log[prevIndex].Term
		for reply.XIndex > rf.log[0].Index {
			if rf.log[reply.XIndex - rf.log[0].Index].Term != T {
				reply.XIndex++
				break
			}
			reply.XIndex--
		}
		reply.XTerm = T
		reply.Success = false
		return
	}

	//*********这里不会出现冲突
	//如果RPC请求中的日志项为空，则说明该RPC请求为Heartbeat
	//如果RPC请求中的日志项为不为空并且日志落后了就与leader进行同步日志
	for idx, entry := range args.Entries {
		//rpc 3
		//如果一个已经存在的条目和新条目（即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同）
		//那么就删除这个已经存在的条目以及它之后的所有条目
		//**************需要对PrevLogIndex一个一个的比对
		if entry.Index <= rf.log[len(rf.log) - 1].Index && rf.log[entry.Index - rf.log[0].Index].Term != entry.Term {
			//如果后面还有日志就会覆盖舍弃（论文5.3，因为后面的不和Leader同步）
			rf.log = rf.log[:entry.Index - rf.log[0].Index] //范围是[0, entry.Index - 1]
			rf.persist()
		}
		//rpc 4
		//追加日志中尚未存在的任何新条目
		//这里rf.log的长度已经变了，所以如果产生了冲突，肯定会进入这个if
		if entry.Index > rf.log[len(rf.log) - 1].Index {
			//添加日志在后面
			rf.log = append(rf.log, args.Entries[idx:]...)
			rf.persist()
			break
		}
	}

	//rpc 5
	//如果领导者的已知已经提交的最高的日志条目的索引leaderCommit 大于 接收者的已知已经提交的最高的日志条目的索引rf.commitIndex
	//令 rf.CommitIndex 等于 LeaderCommit 和 新日志条目索引值中较小的一个（图二）
	//rf.commitIndex与leader同步信息(就是设置rf.commitIndex的值)，便于提交日志应用到状态机
	if args.LeaderCommit > rf.commitIndex {
		//避免len(rf.log) < args.LeaderCommit（日志不够）这种情况，执行日志的时候就会出错
		if rf.log[len(rf.log) - 1].Index < args.LeaderCommit {
			rf.commitIndex = rf.log[len(rf.log) - 1].Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.applyCond.Broadcast() //唤醒go线程，执行提交日志，******如果执行的命令需要很多时间，就会出现commitIndex和lastApplied差距很大
	}
	reply.Success = true
}


//leader发送附加日志或者心跳得到回复以后的处理函数
/*
 * 1.如果返回值中的Term大于leader的Term，证明出现了分区，节点状态转换为follower
 * 2.如果RPC成功的话更新leader对于各个服务器的状态
 * 3.如果RPC失败的话证明两边日志有冲突，使用前面提到的reply.XIndex作为nextIndex，用于请求参数中的PrevLogIndex
 */
func (rf *Raft) handleAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//1、出现网络分区，这是一个落后的leader
	if reply.Term > rf.currentTerm {
		rf.current_state = "FOLLOWER"
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}

	//避免巨恶心的多线程情况，出现网络分区，这是一个落后的leader（上面的if），收到第一个回信走上面的if，第二次回信就会走这里（Term已经改变）
	if args.Term == rf.currentTerm {
		if reply.Success {
			//加上发送的长度，就不需要Follower传过来CommitIndex（Follower的log日志长度）来更新nextIndex和matchIndex了
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			//分布式多线程处理
			if rf.nextIndex[server] < next {
				rf.nextIndex[server] = next
			}
			if rf.matchIndex[server] < match {
				rf.matchIndex[server] = match
			}
		} else { //如果有冲突
			//args.PrevLogIndex大于Follower的Index
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XIndex
			} else {
				//leader收到响应后，找到最后一个term = XTerm的位置，将nextIndex设置为这个位置+1
				lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
				if lastLogInXTerm > 0 {
					//Leader有这个冲突任期的日志
					rf.nextIndex[server] = lastLogInXTerm
				//如果找不到term = XTerm的log，则将nextIndex设置为XIndex
				} else {
					//传过来的XIndex = log中第一条term = XTerm的Index
					//领导人可以减小 nextIndex 越过所有*******那个任期冲突的所有日志条目
					rf.nextIndex[server] = reply.XIndex
				}
			}
		}

		//避免网络延迟
		if rf.current_state != "LEADER" {
			//fmt.Printf("Error in handleAppendEntries, receive a heartbeat reply, but not a leader.")
			return
		}

		//************************日志复制的最关键的一个点(两个阶段)
		//第一阶段：在领导人将创建的日志条目复制到大多数的服务器上的时候，日志条目就会被提交（论文5.3）
		//第二阶段：复制到大部分的服务器上面（没有执行），Leader的CommitIndex就会增加，随后的的心跳就会让FOLLOWER执行
		//这里可以和其他服务器比较matchIndex当到大多数的时候就可以提交这个值
		//leader rule 4
		//如果存在一个满足 N > commitIndex 的 N，并且大多数的 matchIndex[i] ≥ N 成立，并且
		//log[N].term == currentTerm 成立，那么令 commitIndex 等于这个 N （5.3 和 5.4 节）
		for n := rf.commitIndex + 1; n <= rf.log[len(rf.log) - 1].Index; n++ {
			//********************leader 只能提交当前 term 的日志，不能提交旧 term 的日志，重中之重论文5.4.2 图8
			if rf.log[n - rf.log[0].Index].Term != rf.currentTerm {
				continue
			}
			counter := 1
			for serverId := 0; serverId < len(rf.peers); serverId++ {
				if serverId != rf.me && rf.matchIndex[serverId] >= n {
					counter++
				}
				if counter >= len(rf.peers)/2 + 1 {
					rf.commitIndex = n
					//log.Printf("在Term : %d 中, Leader : %d index : %d 的日志已经提交\n", rf.currentTerm, rf.me, n)
					rf.applyCond.Broadcast()	//提交日志，下次心跳的时候会提交follower中的日志（唤醒在等待的线程）
					break
				}
			}
		}
	}
}


func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log[len(rf.log) - 1].Index; i > rf.log[0].Index; i-- {
		term := rf.log[i - rf.log[0].Index].Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

//（执行提交日志）（***************使用异步执行的方式）
//快照和普通的命令不同，需要不同的形式提交
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		//这里就解决了图8的(e)中任期为2的日志的提交
		//如果在崩溃之前，S1 把自己主导的新任期里产生的日志条目复制到了大多数机器上，就如 (e) 中那样
		//那么在后面任期里面这些新的日志条目就会被提交（因为 S5 就不可能选举成功）
		//***********这样在同一时刻就同时保证了，之前的所有老的日志条目就会被提交
		if rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.log[len(rf.log) - 1].Index {
			rf.lastApplied++
			//发送到applyCh通道就相当于执行了这个命令
			applyMsg := ApplyMsg {
				CommandValid : true,
				CommandIndex : rf.lastApplied,
				Command : rf.log[rf.lastApplied- rf.log[0].Index].Command,
				CommandTerm : rf.currentTerm,
			}
			//log.Printf("在Term : %d 中, node : %d 的日志已经执行到 %d\n", rf.currentTerm, rf.me, rf.lastApplied)

			//*************sync.Cond还挺难用的。Wait()方法内部会先释放锁，因此在调用Wait()方法前必须保证持有锁。在唤醒之后会自动持有锁
			//因此Wait()方法之后再不能加锁。rf.applyCh <- applyMsg 这条语句可能会阻塞(导致一直占有锁或者产生死锁)
			//因此执行这条语句前先释放锁，但构建applyMsg的过程需要持有锁
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
//Vote：投票
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//日志心跳的RPC（和投票形式一样）
func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//快照RPC
func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
//我们可以看到Start函数实际的语义其实就是提交一个日志，而之后如何把它同步到Follower，那就是我们的问题了
//这里我的方法就是在心跳包中进行同步，不做额外的逻辑，在应该发送心跳包的时候如果有额外的日志就附带上日志
//如果没有的话就是一个简单的心跳包
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.current_state != "LEADER" { 	//不是Leader拒绝即可，测试代码用这个判断哪一个节点是Leader
		return index, rf.currentTerm, !isLeader
	}
	
	term = rf.currentTerm
	index = rf.log[len(rf.log) - 1].Index + 1

	nlog := Entry {
		Command : command,
		Term : term,
		Index : index,
	}

	//提交一个命令其实就是向日志里面添加一项 在心跳包的时候同步
	rf.log = append(rf.log, nlog)

	//log.Printf("leader append log [leader=%d], [term=%d], [command=%v]\n", rf.me, rf.currentTerm, command)
	//log.Printf("leader append log [leader=%d], [term=%d]\n", rf.me, rf.currentTerm)

	rf.persist()  //2C
	//每次start并不一定要立刻触发AppendEntry
	//理论上如果每次都触发AppendEntry，虽然可以及时的同步log，但是start被调用的频率又超高，Leader就会疯狂发送大量RPC，并且有大量重复的Entries
	//如果不主动触发，而被动的依赖心跳周期，反而可以造成batch operation(分批操作)的效果，将QPS固定成一个相对较小的值
	//当中的trade-off（权衡）需要根据使用场景自己衡量
	//还有一个点是lab3，TestSpeed3A可能会超时，具体测试是单客户端1000次Append操作，再执行一次Get，用来检验结果
	//这里需要修改Raft层，每一次Start()都要去同步日志，否则通过心跳同步太慢了
	rf.SendAppendEntriesToAllFollower(false)

	//实验规定的
	return index, term, isLeader
}


//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
//如果这位peek最近没有收到领导者的心跳信号，自动选举机将开始新的选举
//另外，ticker会一直运行，直到节点被kill，因此集群领导者并非唯一不变，一旦领导者出现了宕机、网络故障等问题，其它节点都能第一时间感知
//并迅速做出重新选举的反应，从而维持集群的正常运行，毕竟Raft集群一旦失去了领导者，就无法工作
//集群开始的时候，所有节点均为Follower，它们依靠ticker()成为Candidate
//ticker计时并且按时间触发leader election或者append entry。
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//这里的代码用于检查是否应该开始领导人选举，并使用time.Sleep()随机化睡眠时间
		//实验里面说了，您需要编写定期或在延迟时间后采取行动的代码。最简单的方法是创建一个带有调用 time.Sleep()的循环的 goroutine
		//不要使用 Go 的time.Timer或time.Ticker，它们很难正确使用（就是可能会有bug）
		time.Sleep(rf.heartbeatTime)
		rf.mu.Lock()
		if rf.current_state == "LEADER" {
			rf.SendAppendEntriesToAllFollower(true)
		} else {
			//*********这里涉及一个选举过程的超时时间：其实也是和electionTime一样
			if time.Now().After(rf.electionTime) {		//也就是electionTime到期选举超时，则发起一轮选举（代替了定时器）
				//因为拉票是异步执行，从而也不阻塞ticker线程，After()比较时间，如果当前时间大于electionTime选举到期时间
				rf.StartElection()
			}
		}
		rf.mu.Unlock()
	}
}

//开始选举
func (rf *Raft) StartElection() {
	rf.current_state = "CANDIDATE"  //如果是改变当前为Candidate
	rf.currentTerm++			    //任期+1
	rf.votedFor = rf.me				//***************首先给自己投票，给自己投完票就不会给别人投了，候选人是不会给别人投票的
	votes_counts := 1				//记录此次投票中获取的票数
	rf.persist()

	//RPC：向其他服务器给自己拉票
	lastLog := rf.log[len(rf.log) - 1]
	args := RequestVoteArgs {
		Term : rf.currentTerm, 			//候选人的任期号
		CandidateId : rf.me,			//请求选票的候选人的 Id
		LastLogIndex : lastLog.Index,   //候选人的最后日志条目的索引值，这一项用于在选举中选出最数据最新的节点 论文[5.4.1]
		LastLogTerm : lastLog.Term,		//候选人最后日志条目的任期号（比较长度、任期号）
	}

	//重置选举超时时间
	rf.resetElectionTimer()

	//log.Printf("{Node %v} starts election with RequestVoteRequest %v", rf.me, args)
	//和其他服务器通信请求投票给自己
	for serverNumber := 0; serverNumber < len(rf.peers); serverNumber++ {
		//当然不需要和自己通信啦
		if serverNumber == rf.me {
			continue
		}

		//并行效率更高，发起投票需要异步进行，从而也不阻塞ticker线程
		//这样candidate 再次 electionTime 超时之后才能自增 term 继续发起新一轮选举
		go func(server int, args RequestVoteArgs, rf *Raft, votes_counts *int) {
			reply := RequestVoteReply{}
			//调用RPC
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				//之前那个ticket线程会释放锁
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//对于获取到的结果进行处理，有三种情况
				//log.Printf("收到拉票回复{Node %v} 请求 %v to {Node %v} 响应 %v in term %v", rf.me, args, server, reply, rf.currentTerm)

				//1、收到的纪元小于当前纪元，无效抛弃这条过期信息，不处理

				//2、收到的纪元大于当前纪元，说明接收到来自新的领导人的响应，转变成跟随者（可能当前节点宕机出现分区，任期落后很多）
				//*************如果那个节点不是Leader，那么后面那个节点不会发送心跳。也意味着当前节点会超时再一次变成候选人进行拉票
				if reply.Term > rf.currentTerm {
					//log.Printf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, server, reply.Term, rf.currentTerm)
					rf.current_state = "FOLLOWER"
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()  //持久化Raft状态
					return
				}
				
				//3、任期相同，判断投票是否有效
				if reply.Term == rf.currentTerm && rf.currentTerm == args.Term {
					//超时时转换成候选者进行选举，其他时候不会进入这个if（如果已经Leader已经选举出来了就不会进入了）
					if rf.current_state == "CANDIDATE" && reply.VoteGranted {
						*votes_counts++  //投票+1
						//log.Printf("{Node %v} votes_counts %v in term %v", rf.me, *votes_counts, rf.currentTerm)
						//票数过半
						//一旦成为领导人：发送空的附加日志 RPC（心跳）给其他所有的服务器；在一定的空余时间之后不停的重复发送，以阻止跟随者超时
						if *votes_counts >= len(rf.peers)/2 + 1 {
							rf.current_state = "LEADER"
							//log.Printf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							//Raft学生指南：设置领导人的日志条目（初始值为领导者最后的日志条目的索引+1）论文中也提到了5.3
							lastLogIndex := rf.log[len(rf.log) - 1].Index
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								rf.nextIndex[i] = lastLogIndex + 1
								rf.matchIndex[i] = 0
							}

							//选举成功之后立即广播（发送心跳）
							rf.SendAppendEntriesToAllFollower(true)
						}
					}
				}
			}
		}(serverNumber, args, rf, &votes_counts)	
	}
}


//
// example RequestVote RPC handler.
//
//Follower投票接收实现
 func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//defer log.Printf("响应拉票{Node %v} state is {%v, %v, %v, %v} 请求 %v 响应 %v", rf.me, rf.current_state, rf.currentTerm, rf.commitIndex, rf.lastApplied, args, reply)
	//持久化状态
	//defer rf.persist()
	
	//1、此节点Term大于请求者，忽略请求
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//比较最后一项日志的Term，相同的话比较索引，判断日志是否比此节点日志新
	voting := false
	if len(rf.log) > 0 {
		myLastLog := rf.log[len(rf.log) - 1]
		if args.LastLogTerm > myLastLog.Term ||
			(args.LastLogTerm == myLastLog.Term &&
				args.LastLogIndex >= myLastLog.Index) {
			voting = true
		}
	}

	//2、此节点Term小于请求者
	if args.Term > rf.currentTerm {
		rf.current_state = "FOLLOWER"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		//********投票给它，任期小于为什么还要判断日志呢，因为一开始请求者任期会+1，但是请求者原来的时候日志可能比当前节点新
		//这个时候此节点肯定还没有进行投票，不然此节点的任期就和其他的CANDIDATE一样的了，就不会出现任期小于请求者
		if voting {
			rf.votedFor = args.CandidateId
		}
		rf.persist()
		//重置选举超时时间
		rf.resetElectionTimer()
		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}

	//3、Term相同，不投票
	//***********如果这个时候该节点也成为了CANDIDATE或者给别人投过票，任期+1就相等了，就不进行投票
	//用来避免多线程编程的其他情况（所以这里基本就不是正常的投票，我们只是做一个处理）
	if args.Term == rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
}


func (rf *Raft) resetElectionTimer() {
	rf.electionTime = time.Now().Add(time.Millisecond * time.Duration(250 + rand.Intn(250)))
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expecats Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
//创建一个新的 Raft 服务器实例 2A
//每一个节点在初始化时，状态为追随者，任期为0，当一定时间内未收到领导者日志后，会自动成为候选者，并给自己投票，且任期+1
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	/*
		peers参数：是通往其他Raft端点处于连接状态下的RPC连接
		me参数：是自己在端点数组中的索引
	*/
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	
	rf.current_state = "FOLLOWER"   //初始状态follower
	rf.currentTerm = 0
	rf.votedFor = -1   //voted：投票赞成
	//可以使用内置make函数创建切片，这就是您创建动态大小的数组的方式，初始长度为0
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{-1, 0, 0})//便于后面的判空，论文也说了log索引从1开始

	//为了降低出现选举失败的概率，选举超时时间随机，**例如：[150,300]ms（论文里面有）
	//节点随机选择超时时间，通常在 [T, 2T] 之间（T = electionTimeout）
	//Raft 的 RPCs 需要接收方将信息持久化的保存到稳定存储中去，所以广播时间大约是 0.5 毫秒到 20 毫秒，取决于存储的技术
	//因此，选举超时时间可能需要在 10 毫秒到 500 毫秒之间（论文5.6）
	//T >> broadcast time(T大于广播时间)时效果更佳论文里面有
	//这样，节点不太可能再同时开始竞选，先竞选的节点有足够的时间来索要其他节点的选票
	//*****设置一个待完成时间点time.Time
	rf.resetElectionTimer()

	//测试者（实验）要求领导者每秒发送心跳 RPC 不超过十次。（即 100ms 发送一次心跳）
	rf.heartbeatTime = time.Duration(100) * time.Millisecond

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))  //记录"每一个"服务器需要发送的下一个日志索引值
	rf.matchIndex = make([]int, len(peers)) //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	//从崩溃前保持的状态来进行初始化(读取持久化数据)
	rf.readPersist(persister.ReadRaftState())
	//崩溃+重启会发生什么？讲座
	//服务从磁盘读取快照，Raft从磁盘读取持久化日志，Raft 将 lastApplied 设置为快照最后包含的索引避免重新应用已经应用的日志条目
	if rf.log[0].Index > 0 {
		rf.lastApplied = rf.log[0].Index
	}
	
	// start ticker goroutine to start elections
	//启动ticker goroutine开始第一轮选举，用来触发 heartbeat timeout 和 election timeout
	go rf.ticker()

	//用来往 applyCh 中 push 提交的日志并保证 exactly once(恰好一次)
	go rf.applier()

	return rf
}

