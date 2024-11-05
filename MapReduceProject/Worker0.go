// client.go
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
	"unicode"
)

// 监督类
type Monitor struct {
}

// 任务反馈类
type ReturnResult struct {
	Order     int    //工人编号
	ResultMap string //接受的map
}

// 工作移交类
type ReturnRest struct {
	Order     int    //工人编号
	ResultMap string //接受的map
	Stemp     string //未统计的片段
	Wordtemp  string //当前正在拼接的单词
}

// string转Map
func JsonToMapWorker(str string) map[string]int {

	var tempMap map[string]int

	err := json.Unmarshal([]byte(str), &tempMap)

	if err != nil {
		panic(err)
	}

	return tempMap
}

// 领取任务
func JobFetch(temp *string, order int) {

	for {
		// 建立连接
		client, err := rpc.DialHTTP("tcp", "localhost:9090")
		if err != nil {
			//panic(err.Error())
			//fmt.Println("连接失败")
			time.Sleep(time.Second)
			continue
		}

		// 同步调用
		err = client.Call("Connection.Distribute", order, &temp)
		if err != nil {
			panic(err.Error())
		}
		fmt.Println(order, "已领取到文件")
		client.Close()
		break
	}

}

// 统计单词频数
func WordCount(temp *string, order int, words *map[string]int) bool {
	//获取当前时间
	now1 := time.Now().UnixMicro()
	var word string // 定义一个变量用来接收单词
	word = ""
	for i, w := range *temp { // 遍历字符串
		//判定是否超时
		if OverTime[order] {
			//进行任务转移，联系master
			var transfer ReturnRest
			transfer.Order = order
			transfer.ResultMap = MapToJson(*words)
			transfer.Wordtemp = word
			transfer.Stemp = (*temp)[i:len(*temp)]
			RestHandIn(&transfer, order)
			fmt.Println(order, "进行任务转移，已经联系master")
			return false //未能在规定时间内完成任务
		}
		if !unicode.IsLetter(w) { // 遇到非字母元素，说明一个单词结束，将单词存入，并且重置word变量
			if word != "" { // 由于可能存在两个或者多个的非字母字符相邻，所以在录入之前应该进行一次判断
				(*words)[word]++
			}
			word = ""
			continue
		} else {
			word = fmt.Sprintf("%s%c", word, w) // 字母元素拼接到word后面，组成单词
		}

	}
	now2 := time.Now().UnixMicro()
	fmt.Println("序号：", order, "执行时间", now2-now1)
	fmt.Println("序号：", order, "完成单词统计")

	return true //表示在规定时间内完成任务

}

// 反馈结果
func ResultHandIn(rr ReturnResult, order int) {
	// 建立连接
	client, err := rpc.DialHTTP("tcp", "localhost:9090")
	if err != nil {
		panic(err.Error())
	}

	var temp string

	//同步调用
	err = client.Call("Connection.GetResult", rr, &temp)
	if err != nil {
		panic(err.Error())
	}
	if temp == "yes" {
		fmt.Println(order, "反馈成功")

	}
	client.Close()
}

// 提交未完成任务
func RestHandIn(rr *ReturnRest, order int) {
	// 建立连接
	client, err := rpc.DialHTTP("tcp", "localhost:9090")
	if err != nil {
		panic(err.Error())
	}

	var temp string

	//同步调用
	err = client.Call("Connection.GetRest", rr, &temp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(order, temp)

	client.Close()
}

// 接受master检查
func (mon *Monitor) ConWithMaster(req int, resp *string) error {
	//按序号分配
	OverTime[req] = true
	*resp = strconv.Itoa(req) + "即将转移任务"
	return nil
}

// 接受剩余任务
func (mon *Monitor) ReceiveRest(rr ReturnRest, resp *string) error {
	//添加剩余任务
	waitingqueueWorker = append(waitingqueueWorker, rr)
	*resp = strconv.Itoa(rr.Order) + "的剩余任务被接收"
	return nil
}

// 处理被master分配的别的worker的工作
func WorkOverTime() {
	for {
		if len(waitingqueueWorker) > 0 {
			//任务解读
			var word string = waitingqueueWorker[0].Wordtemp
			var order int = waitingqueueWorker[0].Order
			var stemp string = waitingqueueWorker[0].Stemp
			var maptemp map[string]int = JsonToMapWorker(waitingqueueWorker[0].ResultMap)

			//踢出队列
			waitingqueueWorker = waitingqueueWorker[1:]
			//继续处理
			for _, w := range stemp { // 遍历字符串
				if !unicode.IsLetter(w) { // 遇到非字母元素，说明一个单词结束，将单词存入，并且重置word变量
					if word != "" { // 由于可能存在两个或者多个的非字母字符相邻，所以在录入之前应该进行一次判断
						maptemp[word]++
					}
					word = ""
					continue
				} else {
					word = fmt.Sprintf("%s%c", word, w) // 字母元素拼接到word后面，组成单词
				}
			}
			time.Sleep(time.Second)
			//提交任务
			var rr ReturnResult
			rr.Order = order
			rr.ResultMap = MapToJson(maptemp)
			ResultHandIn(rr, order)
			fmt.Println("完成并反馈", order, "剩余任务")
		}
		time.Sleep(time.Second)

	}
}

// 接受master检查途径
func MoniterPath() {
	// 功能对象注册
	mon := new(Monitor)
	err := rpc.Register(mon) //自定义服务名
	if err != nil {
		panic(err.Error())
	}
	// HTTP注册
	rpc.HandleHTTP()
	// 端口监听
	listen, err := net.Listen("tcp", ":8081")
	if err != nil {
		panic(err.Error())
	}
	// 启动服务
	_ = http.Serve(listen, nil)
}

// worker端工作程序
func Worker(wgtemp *sync.WaitGroup, order int) {
	defer wgtemp.Done()
	var receivejob string
	//开始领取任务
	JobFetch(&receivejob, order)
	//开始统计单词频数
	var result map[string]int
	result = make(map[string]int)
	if WordCount(&receivejob, order, &result) { //若在规定时间内完成，进行反馈
		//反馈
		var rr ReturnResult
		rr.Order = order
		rr.ResultMap = MapToJson(result)
		ResultHandIn(rr, order)
	}

}

// map转String
func MapToJson(param map[string]int) string {
	dataType, _ := json.Marshal(param)
	dataString := string(dataType)
	return dataString
}

// 超时队列，存储每台机器是否超时,实际上每台机器仅需存储自己的一个值，此处由于多线程需要用切片存储
var OverTime []bool

// 等待队列，储存待办任务
var waitingqueueWorker []ReturnRest

func main() {

	//超时队列初设
	OverTime = []bool{false, false, false, false, false, false, false, false, false, false}
	//开启加班监听
	go WorkOverTime()
	//开启worker端服务器
	go MoniterPath()
	//fmt.Println("多线程模拟多台机器连接")
	wg := sync.WaitGroup{}
	wg.Add(10)
	//分别向master领取任务

	for i := 0; i < 10; i++ {
		go Worker(&wg, i)
	}
	wg.Wait()
	fmt.Println("任务反馈结束")
	wg2 := sync.WaitGroup{} //确保主线程不会停止，实现每时每刻监听是否被分配剩余任务
	wg2.Add(1)
	wg2.Wait()

}
