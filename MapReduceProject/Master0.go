// server.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// 通讯类
type Connection struct {
}

// 任务接收类
type ReceiveResult struct {
	Order     int    //工人编号
	ResultMap string //接受的map
}

// 工作移交类
type ReturnRestMaster struct {
	Order     int    //工人编号
	ResultMap string //接受的map
	Stemp     string //未统计的片段
	Wordtemp  string //当前正在拼接的单词
}

// 对已有切片按worker序号进行进行分配方法
func (con *Connection) Distribute(req int, resp *string) error {
	tcurrent := time.Now().UnixMicro()
	//设置开始时间
	StartTime[req] = tcurrent
	//按序号分配
	*resp = sgroup[req]
	fmt.Println(req, "已领取")
	return nil
}

// 收集返回结果
func (con *Connection) GetResult(rr *ReceiveResult, resp *string) error {

	//类型转换
	results[rr.Order] = JsonToMap(rr.ResultMap)
	*resp = "yes"
	numberofresults++
	fmt.Println("接收到：", rr.Order, "的反馈")
	Finish[rr.Order] = true //确认接受
	return nil
}

// 收集剩余任务
func (con *Connection) GetRest(rr *ReturnRestMaster, resp *string) error {
	waitingqueue = append(waitingqueue, *rr)
	*resp = "已经接收到" + strconv.Itoa(rr.Order) + "的剩余任务"
	fmt.Println("收集到了", rr.Order, "的剩余任务,长度为： ", len(waitingqueue))
	return nil
}

// 与工人通讯
func connectionWithWorker() {
	// 功能对象注册
	con := new(Connection)
	err := rpc.Register(con) //自定义服务名rpc
	if err != nil {
		panic(err.Error())
	}
	// HTTP注册
	rpc.HandleHTTP()

	// 端口监听
	listen, err := net.Listen("tcp", ":9090")
	if err != nil {
		panic(err.Error())
	}
	// 启动服务
	_ = http.Serve(listen, nil)
}

// 按行读取文件并存储到s切片中
func GetLine(arraytemp *[]string) {

	// 打开文件
	file, err := os.Open("sample/sample3.txt")
	if err != nil {
		fmt.Println("打开文件失败：", err)
		return
	}
	defer file.Close()

	// 创建 scanner 对象，用于逐行扫描文件
	scanner := bufio.NewScanner(file)

	// 逐行读取文件并存储
	for scanner.Scan() {
		//fmt.Println(scanner.Text())

		*arraytemp = append(*arraytemp, scanner.Text())
		*arraytemp = append(*arraytemp, "\n ")

	}

	// 检查是否有错误发生
	if err = scanner.Err(); err != nil {
		fmt.Println("扫描文件出错：", err)
	}
}

// 将s根据worker数量进行合并分组，设定10个
func Grouping(s []string) {

	common := len(s) / 9 //每组行数,10号补漏
	//分组，存储到sgroup
	for i := 0; i < len(s); i++ {

		//fmt.Println("组号： ", i/common)
		sgroup[i/common] += s[i]
	}

}

// 对从worker收集到的结果进行汇总
func ShuffleAndReduce() {

	for {
		if numberofresults == 10 {
			SaveIntermediateResult()
			for i := 0; i < 10; i++ {
				for key, value := range results[i] {
					finalresult[key] += value
				}

			}
			fmt.Println("汇总成功")
			SaveFinalResult()
			break

		}
		//当前线程休眠一秒
		time.Sleep(time.Second)
	}

}

// 存储中间结果
func SaveIntermediateResult() {
	fmt.Println("全部结果收集完成")
	fmt.Println("写入中间结果文件")
	for i := 0; i < 10; i++ {

		temp := "intermediateresult/worker"
		temp += strconv.Itoa(i)
		temp += ".txt"
		filePath := temp
		file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			fmt.Println("文件打开失败", err)
		}
		//及时关闭file句柄
		defer file.Close()
		//写入文件时，使用带缓存的 *Writer
		write := bufio.NewWriter(file)
		for key, value := range results[i] {

			write.WriteString(key + ":" + strconv.Itoa(value) + "\n")

		}
		//Flush将缓存的文件真正写入到文件中
		write.Flush()
	}
}

// 储存最终结果
func SaveFinalResult() {
	//写入文件
	temp := "finalresult/finalresult.txt"
	filePath := temp
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("文件打开失败", err)
	}
	//及时关闭file句柄
	defer file.Close()
	//写入文件时，使用带缓存的 *Writer
	write := bufio.NewWriter(file)
	for key, value := range finalresult {

		write.WriteString(key + ":" + strconv.Itoa(value) + "\n")

	}
	//Flush将缓存的文件真正写入到文件中
	write.Flush()

}

// 监听第i号worker工作时间
func TimeLisener(order int) {
	for {
		if (StartTime[order] != 0) && (!Finish[order]) { //任务已经开始且结果未提交，要求工作时间不超过10秒
			tcurrent := time.Now().UnixMicro() //获取当前时间

			if tcurrent-StartTime[order] > TIMELIMIT {
				fmt.Println(order, " 超时了")
				SetOverTime(order)
				break
			}
		}
		time.Sleep(time.Microsecond)
	}
}

// 设置第i号工人是否超时
func SetOverTime(order int) {
	for {
		// 建立连接
		client, err := rpc.DialHTTP("tcp", "127.0.0.1:8081")
		if err != nil {
			panic(err.Error())
			//fmt.Println("与", order, "连接失败")
			time.Sleep(time.Microsecond)
			continue
		}

		var temp string
		// 同步调用
		err = client.Call("Monitor.ConWithMaster", order, &temp)
		if err != nil {
			panic(err.Error())
		}
		fmt.Println(order, "工作结果", temp)
		client.Close()
		break
	}
}

// 移交剩余任务
func AssignRest() {
	for {
		if len(waitingqueue) > 0 { //等待重新分配任务队列不为空
			//分配任务
			for {
				// 建立连接
				client, err := rpc.DialHTTP("tcp", "127.0.0.1:8081")
				if err != nil {
					panic(err.Error())
					//fmt.Println("与", order, "连接失败")
					time.Sleep(time.Microsecond)
					continue
				}

				var temp string
				// 同步调用
				err = client.Call("Monitor.ReceiveRest", waitingqueue[0], &temp)
				if err != nil {
					panic(err.Error())
				}
				fmt.Println(temp)
				fmt.Println("已经分配", waitingqueue[0].Order, "的剩余任务")
				//踢出队列
				waitingqueue = waitingqueue[1:]
				client.Close()
				break
			}
		}
		time.Sleep(time.Second)
	}
}

// string转Map
func JsonToMap(str string) map[string]int {

	var tempMap map[string]int

	err := json.Unmarshal([]byte(str), &tempMap)

	if err != nil {
		panic(err)
	}

	return tempMap
}

// 分组后切片
var sgroup []string

// 从worker们收到的结果份数
var numberofresults int

// 各个工人统计结果切片
var results []map[string]int

// 最终结果集
var finalresult map[string]int

// worker开始工作时间集合
var StartTime []int64

// 工人是否完成队列
var Finish []bool

// 等待队列，储存待办任务
var waitingqueue []ReturnRestMaster

// 规定每个工人工作时间限制
const TIMELIMIT int64 = 10000

func main() {

	//初设工人完成队列
	Finish = []bool{false, false, false, false, false, false, false, false, false, false}
	//初设开始时间队列
	StartTime = make([]int64, 10)
	for i := 0; i < 10; i++ {
		StartTime[i] = 0
	}
	results = make([]map[string]int, 10)
	//监听剩余任务队列
	go AssignRest()
	//开始监听每个worker工作时间
	for i := 0; i < 10; i++ {
		go TimeLisener(i)
	}
	//初始化结果份数
	numberofresults = 0
	//逐行读取的切片
	var sline []string
	sgroup = make([]string, 10)        //初始化切片,10组
	finalresult = make(map[string]int) //初始化最终结果集
	GetLine(&sline)                    //按行读取文件
	Grouping(sline)                    //分组
	go ShuffleAndReduce()              //开启线程监听，在全部工人提交结果后汇总结果
	connectionWithWorker()             //与worker展开通讯

}
