package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import (
    "os"
	"sort"
	"strings"
    "io/ioutil"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key. //根据Key进行排序
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
//使用ihash(key) % NReduce为映射发出的每个键值选择reduce任务编号
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//不断循环向coordinator请求工作
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	//单机运行，直接使用 PID 作为 Worker ID，方便 debug
	//PID进程标识符，是大多数操作系统的内核用于唯一标识进程的一个数值
	//pid := strconv.Itoa(os.Getpid())
	pid := os.Getpid()
	log.Printf("Worker %d started: \n", pid)
	
    lastTaskType := ""
    lastTaskId := -1
	//进入循环，向Coordinator申请Task
	for {
		//定义RPC的任务申请类型和任务回复类型
		args := ApplyForTaskArgs{
			WorkerId : pid,
			LastTaskId : lastTaskId,
			LastTaskType : lastTaskType,
		}
		reply := ApplyForTaskReply{}
		//通过RPC来调用Coordinator.ApplyForTask()
		//来拿到回复的任务类型对象
		call("Coordinator.ApplyForTask", &args, &reply)

		if reply.TaskType == "" {
			//MR作业已完成，退出
			//从协调器接收到作业完成信号
			log.Printf("Received job finish signal from coordinator")
			break;
		}else if reply.TaskType == MAP {
			//处理MAP TASK
			//读取输入数据
			//打开文件
			file, err := os.Open(reply.MapInputFile)
			if err != nil {
				log.Fatalf("Failed to read map input file %s: %e", reply.MapInputFile, err)
			}
			//读取文件里面的内容
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Failed to read map input file %s: %e", reply.MapInputFile, err)
			}
			//传递动态链接的mapf函数(MAP操作函数)，得到中间结果
			//[]KeyValue类型
			kva := mapf(reply.MapInputFile, string(content))
			//对中间结果进行分桶
			//可以把相同的key交给一个Rudece处理
			//key是int类型，value是[]KeyValue切片(数组具有固定大小，切片是数组元素的动态大小、灵活的视图)
			hashedKva := make(map[int][]KeyValue)
			for _, kv := range kva {
				hashed := ihash(kv.Key) % reply.NReduce
				//kv加入到[]KeyValue切片后面
				hashedKva[hashed] = append(hashedKva[hashed], kv)
			}

			//MAP操作结束，写出中间结果文件NRudece个
			for i := 0; i < reply.NReduce; i++ {
				file, _ := os.Create(tmpMapOutFile(pid, reply.TaskId, i))
				//遍历每一个桶中的所有kv
				for _, kv := range hashedKva[i] {
					//写入的类型（一行一对KV，中间用"\t"隔开），等会Reduce取出的时候也是用这种格式
					fmt.Fprintf(file, "%v\t%v\n", kv.Key, kv.Value)
				}
				file.Close()
			}

		}else {
			//处理REDUCE Task
			//读取所有属于该 REDUCE Task 的中间结果文件数据，对所有中间结果进行排序，并按 Key 值进行归并
			var lines []string
			for i := 0; i < reply.NMap; i++ {
				//读取每一个Map生成的NRudece文件中路由到这个repiy.TaskId[0, NRudece]Reduce中的文件
				inputFile := finalMapOutFile(i, reply.TaskId)
				file, err := os.Open(inputFile)
				if err != nil {
					log.Fatalf("Failed to open map output file %s: %e", inputFile, err)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Failed to read map output file %s: %e", inputFile, err)
				}

				//调用append函数必须用原来的切片变量接收返回值，append追加元素，原来的底层数组装不下的时候
				//Go就会创建新的底层数组来保存这个切片
				//这里将将文件里面的内容根据"\n"分隔(因为mapf函数MAP操作就是这样分隔的)加入到lines切片后面
				lines = append(lines, strings.Split(string(content), "\n")...)
			}

			var kva []KeyValue
			//遍历每一行（每一个对KV）K和V使用制表符"\t"隔开
			for _, line := range lines {
				//去除一个字符串中的空格
				if strings.TrimSpace(line) == "" {
					continue
				}
				//\t是制表符tab空格字符
				parts := strings.Split(line, "\t")
				kva = append(kva, KeyValue{
					Key : parts[0],
					Value : parts[1],
				})
			}

			//根据Key进行排序
			//ByKey()函数在mrsequential.go里面实现了
			sort.Sort(ByKey(kva))

			//按照相同的Key对中间结果进行并归，传递至Reduce函数
			ofile, _ := os.Create(tmpReduceOutFile(pid, reply.TaskId))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				//将相同Key的Value加到一个集合
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				//调用Reduce函数(将value合并)
				output := reducef(kva[i].Key, values)
				//写出至临时结果文件
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()

		}
		//记录已完成 Task 的信息，在下次 RPC 调用时捎带给 Coordinator
		lastTaskType = reply.TaskType
		lastTaskId = reply.TaskId
		log.Printf("Finished %s task %d", reply.TaskType, reply.TaskId)
	}

	log.Printf("Worker %d exit\n", pid)

	// uncomment to send the Example RPC to the coordinator.
	//取消注释以将示例RPC发送到协调器
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
