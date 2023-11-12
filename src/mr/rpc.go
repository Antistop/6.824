package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//
import "os"
import "strconv"
import "time"
import "fmt"

const (
	MAP = "MAP"
	REDUCE = "REDUCE"
	DONE = "DONE" //完成的
)

//一定要大写开头，不然RPC通信过程序列化/反序列化的时候可能找不到
//任务描述
type Task struct {
	Id int
	Type string //任务类型
	WorkerId int //没有分配就是-1
	MapInputFile string //输入的文件
	DeadLine time.Time //在 Task 超出 10s 仍未完成时，将该 Task 重新分配给其他 Worker 重试
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//示例演示如何声明RPC的参数和应答

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
//添加您的RPC定义
//任务申请
type ApplyForTaskArgs struct{
	WorkerId int
	LastTaskId int //上一个完成的 Task 的 Index，可能为空
	LastTaskType string //上一个完成的 Task 的类型，可能为空
}

//任务回复
type ApplyForTaskReply struct{
	TaskId int
	TaskType string //新 Task 的类型及 Index，若为空则代表 MR 作业已完成，Worker 可退出
	MapInputFile string //MAP TASK才有的(输入的文件)
	NReduce int //MAP才有用于生成中间结果文件
	NMap int //reduce才有，用于生成对应中间结果文件的文件名
}


//文件的保存的名字
//Worker超时未完成的话，Coordinator会把任务分配给其他worker，会产生两个Worker完成同一个任务
//如果work直接将结果写入文件，会出现冲突（如果一个任务产生了多个输出文件，我们没有提供类似两阶段提交的原子操作(分布式)支持这种情况
//因此，对于会产生多个输出文件、并且对于跨文件有一致性要求的任务，都必须是确定性的任务）
//Worker 在写出数据时可以先写出到临时文件，最终确认没有问题后再将其重命名为正式结果文件
//区分开了 Write 和 Commit 的过程。Commit 的过程可以是 Coordinator 来执行，也可以是 Worker 来执行

//Map输出的中间文件
func tmpMapOutFile(workerId int, mapId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d", workerId, mapId, reduceId)
 }
 
 //Map最终输出的文件
 func finalMapOutFile(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
 }
 
 //Reduce输出的中间文件
 func tmpReduceOutFile(workerId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d", workerId, reduceId)
 }
 
//Reduce最终输出的文件
 func finalReduceOutFile(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
 }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
