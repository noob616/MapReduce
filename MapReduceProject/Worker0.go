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

// Monitor class
type Monitor struct {
}

// Task feedback class
type ReturnResult struct {
	Order     int    // Worker ID
	ResultMap string // Received map
}

// Handover work class
type ReturnRest struct {
	Order     int    // Worker ID
	ResultMap string // Received map
	Stemp     string // Unprocessed segment
	Wordtemp  string // Current word being formed
}

// Convert string to map
func JsonToMapWorker(str string) map[string]int {

	var tempMap map[string]int

	err := json.Unmarshal([]byte(str), &tempMap)

	if err != nil {
		panic(err)
	}

	return tempMap
}

// Fetch job
func JobFetch(temp *string, order int) {
	for {
		// Establish connection
		client, err := rpc.DialHTTP("tcp", "localhost:9090")
		if err != nil {
			// panic(err.Error())
			// fmt.Println("Connection failed")
			time.Sleep(time.Second)
			continue
		}

		// Synchronous call
		err = client.Call("Connection.Distribute", order, &temp)
		if err != nil {
			panic(err.Error())
		}
		fmt.Println(order, "has received the file")
		client.Close()
		break
	}
}

// Count word frequency
func WordCount(temp *string, order int, words *map[string]int) bool {
	// Get current time
	now1 := time.Now().UnixMicro()
	var word string // Variable to hold the word
	word = ""
	for i, w := range *temp { // Iterate through string
		// Check for timeout
		if OverTime[order] {
			// Transfer task, contact master
			var transfer ReturnRest
			transfer.Order = order
			transfer.ResultMap = MapToJson(*words)
			transfer.Wordtemp = word
			transfer.Stemp = (*temp)[i:len(*temp)]
			RestHandIn(&transfer, order)
			fmt.Println(order, "is transferring task, has contacted master")
			return false // Task not completed within time limit
		}
		if !unicode.IsLetter(w) { // Non-letter encountered, indicating end of a word
			if word != "" { // Check for consecutive non-letter characters
				(*words)[word]++
			}
			word = ""
			continue
		} else {
			word = fmt.Sprintf("%s%c", word, w) // Append letter to word
		}
	}
	now2 := time.Now().UnixMicro()
	fmt.Println("Order:", order, "Execution time:", now2-now1)
	fmt.Println("Order:", order, "Word counting completed")

	return true // Indicates task completed within time limit
}

// Submit result
func ResultHandIn(rr ReturnResult, order int) {
	// Establish connection
	client, err := rpc.DialHTTP("tcp", "localhost:9090")
	if err != nil {
		panic(err.Error())
	}

	var temp string

	// Synchronous call
	err = client.Call("Connection.GetResult", rr, &temp)
	if err != nil {
		panic(err.Error())
	}
	if temp == "yes" {
		fmt.Println(order, "feedback successful")
	}
	client.Close()
}

// Submit incomplete tasks
func RestHandIn(rr *ReturnRest, order int) {
	// Establish connection
	client, err := rpc.DialHTTP("tcp", "localhost:9090")
	if err != nil {
		panic(err.Error())
	}

	var temp string

	// Synchronous call
	err = client.Call("Connection.GetRest", rr, &temp)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(order, temp)

	client.Close()
}

// Accept master's check
func (mon *Monitor) ConWithMaster(req int, resp *string) error {
	// Assign by order
	OverTime[req] = true
	*resp = strconv.Itoa(req) + " is about to transfer task"
	return nil
}

// Accept remaining tasks
func (mon *Monitor) ReceiveRest(rr ReturnRest, resp *string) error {
	// Add remaining tasks
	waitingqueueWorker = append(waitingqueueWorker, rr)
	*resp = strconv.Itoa(rr.Order) + "'s remaining task has been received"
	return nil
}

// Handle work assigned to other workers by master
func WorkOverTime() {
	for {
		if len(waitingqueueWorker) > 0 {
			// Decode task
			var word string = waitingqueueWorker[0].Wordtemp
			var order int = waitingqueueWorker[0].Order
			var stemp string = waitingqueueWorker[0].Stemp
			var maptemp map[string]int = JsonToMapWorker(waitingqueueWorker[0].ResultMap)

			// Remove from queue
			waitingqueueWorker = waitingqueueWorker[1:]
			// Continue processing
			for _, w := range stemp { // Iterate through string
				if !unicode.IsLetter(w) { // Non-letter encountered, indicating end of a word
					if word != "" { // Check for consecutive non-letter characters
						maptemp[word]++
					}
					word = ""
					continue
				} else {
					word = fmt.Sprintf("%s%c", word, w) // Append letter to word
				}
			}
			// Submit task
			var rr ReturnResult
			rr.Order = order
			rr.ResultMap = MapToJson(maptemp)
			ResultHandIn(rr, order)
			fmt.Println("Completed and feedback sent for", order, "remaining task")
		}
		time.Sleep(time.Second)
	}
}

// Accept master's check path
func MoniterPath() {
	// Register the functionality object
	mon := new(Monitor)
	err := rpc.Register(mon) // Custom service name
	if err != nil {
		panic(err.Error())
	}
	// HTTP registration
	rpc.HandleHTTP()
	// Port listening
	listen, err := net.Listen("tcp", ":8081")
	if err != nil {
		panic(err.Error())
	}
	// Start service
	_ = http.Serve(listen, nil)
}

// Worker endpoint program
func Worker(wgtemp *sync.WaitGroup, order int) {
	defer wgtemp.Done()
	var receivejob string
	// Start fetching tasks
	JobFetch(&receivejob, order)
	// Start counting word frequency
	var result map[string]int
	result = make(map[string]int)
	if WordCount(&receivejob, order, &result) { // If completed within the time limit, provide feedback
		// Feedback
		var rr ReturnResult
		rr.Order = order
		rr.ResultMap = MapToJson(result)
		ResultHandIn(rr, order)
	}
}

// Convert map to string
func MapToJson(param map[string]int) string {
	dataType, _ := json.Marshal(param)
	dataString := string(dataType)
	return dataString
}

// Timeout queue, storing whether each machine is timed out; in practice, each machine only needs to store its own value, but due to multithreading, a slice is used
var OverTime []bool

// Waiting queue, storing pending tasks
var waitingqueueWorker []ReturnRest

func main() {
	// Initialize timeout queue
	OverTime = []bool{false, false, false, false, false, false, false, false, false, false}
	// Start overtime listener
	go WorkOverTime()
	// Start worker endpoint server
	go MoniterPath()
	// fmt.Println("Multithreading simulating multiple machines connecting")
	wg := sync.WaitGroup{}
	wg.Add(10)
	// Fetch tasks from the master
	for i := 0; i < 10; i++ {
		go Worker(&wg, i)
	}
	wg.Wait()
	fmt.Println("Task feedback complete")
	wg2 := sync.WaitGroup{} // Ensure the main thread does not stop, continuously listening for remaining tasks
	wg2.Add(1)
	wg2.Wait()
}
