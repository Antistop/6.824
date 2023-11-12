package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import (
	"math"
	"time"
	"sync"
	"fmt"
)

//https://pdos.csail.mit.edu/6.824/  6.824课程官网

//https://mr-dai.github.io/mit-6824-lab1
//https://blog.51cto.com/u_15127667/4282658
//在 early exit 中 test-mr.sh 中有一个 wait -n的命令，mac执行会报错，把它改为 wait 了

type Coordinator struct {
	// Your definitions here.
    lock sync.Mutex // 保护共享信息，避免并发冲突
    stage          string //当前作业阶段，MAP or REDUCE，为空代表已完成可退出
    nMap           int  //总 MAP Task 数量
    nReduce        int  //总 Reduce Task 数量
    tasks          map[string]Task //所有仍未完成的 Task 及其所属的 Worker 和 Deadline（若有），使用 Golang Map 结构实现
    availableTasks chan Task //待完成任务，用于响应 Worker 的申请及 Failover 时的重新分配，采用通道实现，内置锁结构
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//示例RPC处理程序
//RPC参数和应答类型在RPC.go中定义
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//基于 Task 的类型和 Index 值生成唯一 Id
//Generate(生成)
func GenTaskId(t string, index int) string {
    return fmt.Sprintf("%s-%d", t, index)
}


// Your code here -- RPC handlers for the worker to call.
//RPC的处理入口，由Worker调用，可用Task获取域分配
//传过来的RPC的任务申请类型和任务回复类型
func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	if(args.LastTaskId != -1){
		//记录Worker的上一个任务完成
		c.lock.Lock();
		tId := GenTaskId(args.LastTaskType, args.LastTaskId);
		//****这里才产生最后的输出结果，是因为怕超时worker和合法worker都写入，造成冲突
		//判断该 Task 是否仍属于该 Worker，如果已经被重新分配则直接忽略（这样超时的就不会进入这个判断里面，也就不会重命名文件）
		//拿着GenTaskId去Map里面找对应的WorkerId，通过二值分配测试一个键是否存在（exists）
		//如果key在m，ok就是true。如果不是，ok是false
		if task, exists := c.tasks[tId]; exists && task.WorkerId == args.WorkerId {
			log.Printf("Mark %s task %d as finished on worker %d\n", task.Type, task.Id, args.WorkerId)
			//将该 Worker 的临时产出文件标记为最终产出文件(commit)
			if args.LastTaskType == MAP {
				//把Map分成 R 个 Rudece，通过分区函数分成 R 个区域，之后周期性的写入到本地磁盘上(就要生成的R个文件)
				for i := 0; i < c.nReduce; i++ {
					//os.Rename(oldname, newname string) 函数可用于重命名文件或文件夹
					err := os.Rename(
						tmpMapOutFile(args.WorkerId, args.LastTaskId, i),
						finalMapOutFile(args.LastTaskId, i))
					if err != nil {
						//标记失败
						log.Fatalf("Failed to mark map output file `%s` as final: %e",
						tmpMapOutFile(args.WorkerId, args.LastTaskId, i), err)
					}
				}
			}else if args.LastTaskType == REDUCE {
				//一个Rudece生成一个文件
				err := os.Rename(
					tmpReduceOutFile(args.WorkerId, args.LastTaskId),
					finalReduceOutFile(args.LastTaskId))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(args.WorkerId, args.LastTaskId), err)
				}
			}
			//map里面的任务移除
			delete(c.tasks, tId)
			//当前阶段所有 Task 已完成，进入下一阶段
			if len(c.tasks) == 0{
				c.cutover();
			}
		}
		c.lock.Unlock();
	}

	//获取一个可用的Task并返回
	//chan内置锁结构
	task, ok := <- c.availableTasks
	if !ok {
		//Chan 关闭，代表整个 MR 作业已完成，通知 Worker 退出
		return nil
	}

	//获取一个可用 Task 并返回
	c.lock.Lock()
	//defer(推迟) 语句将函数的执行推迟到周围的函数返回
	defer c.lock.Unlock()
	log.Printf("Assign %s task %d to worker %d\n", task.Type, task.Id, args.WorkerId)
	//更新task
	task.WorkerId = args.WorkerId
	//设置一个待完成时间
	task.DeadLine = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskId(task.Type, task.Id)] = task
	//给work返回数据
	reply.TaskId = task.Id
	reply.TaskType = task.Type
	reply.MapInputFile = task.MapInputFile
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce

	//修改了引用的值即可，其他的由RPC来负责
	return nil;
}

//当前阶段所有 Task 已完成，进入下一阶段
func (c *Coordinator) cutover(){
	if c.stage == MAP {
		log.Printf("所有的MAP任务已经完成！开始REDUCE任务！")
		c.stage = REDUCE
		for i := 0; i < c.nReduce; i++ {
			//把任务加入到Map、和chan里面进行Reduce操作
			task := Task{
				Id : i,
				Type : REDUCE,
				WorkerId : -1,
			}
			c.tasks[GenTaskId(task.Type, task.Id)] = task
			c.availableTasks <- task
		}
	}else if c.stage == REDUCE {
		//REDUCE Task 已全部完成，MR 作业已完成，准备退出
		log.Printf("所有的REDUCE任务已经完成！")
		close(c.availableTasks) //关闭 Channel，响应所有正在同步等待的 RPC 调用
		c.stage = DONE
	}
}


//
// start a thread that listens for RPCs from worker.go
// 启动一个线程从worker.go侦听RPC
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
//main/mrcoordinator.go定期调用Done()查找
//如果整个工作已经完成
func (c *Coordinator) Done() bool {
	//ret := false
	// Your code here.
	c.lock.Lock()
	ret := c.stage == DONE
	defer c.lock.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// 创建Coordinator，会被mrCoordinator调用
//Reduce的数量和 Coordinator 启动时的 MAP Task 生成
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage : MAP, //MAP作业阶段
		nMap : len(files),
		nReduce : nReduce,
		tasks : make(map[string]Task), //初始化
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	// Your code here.
	//每个输入文件生成一个MAP TASK
	for i, file := range files {
		task := Task{
			Type : MAP,
			Id : i,
			WorkerId : -1,
			MapInputFile: file,
		}
		//基于 Task 的类型和 Index 值生成唯一 ID
		c.tasks[GenTaskId(task.Type, task.Id)] = task
		c.availableTasks <- task
	}

	//启动 Coordinator，开始响应 Worker 请求
    log.Printf("Coordinator start\n")
    c.server()

	//多线程启动自动回收机制，回收超时任务
	//考虑到该过程需要对已分配的 Task 进行周期巡检
	//我们直接在 Coordinator 启动时启动一个 Goroutine(协程)（Go 运行时管理的轻量级线程）来实现
	go func() {
		for {
			//每过500ms执行一次
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			//map遍历：for k, v := range scene
			//将不需要遍历键使用_改为匿名变量形式
			for _, task := range c.tasks {
				if task.WorkerId != -1 && time.Now().After(task.DeadLine) {
					log.Printf("%d 运行任务 %s %d 出现故障，重新收回！", task.WorkerId, task.Type, task.Id)
					//设置为初始状态
					task.WorkerId = -1;
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}

	}()

	return &c
}
