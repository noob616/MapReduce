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

// Communication class
type Connection struct {
}

// Task reception class
type ReceiveResult struct {
	Order     int    // Worker ID
	ResultMap string // Received map
}

// Work handover class
type ReturnRestMaster struct {
	Order     int    // Worker ID
	ResultMap string // Received map
	Stemp     string // Uncounted segments
	Wordtemp  string // Currently concatenating word
}

// Method to distribute tasks based on worker ID
func (con *Connection) Distribute(req int, resp *string) error {
	tcurrent := time.Now().UnixMicro()
	// Set start time
	StartTime[req] = tcurrent
	// Distribute based on ID
	*resp = sgroup[req]
	fmt.Println(req, "has received")
	return nil
}

// Collect returned results
func (con *Connection) GetResult(rr *ReceiveResult, resp *string) error {

	// Type conversion
	results[rr.Order] = JsonToMap(rr.ResultMap)
	*resp = "yes"
	numberofresults++
	fmt.Println("Received feedback from:", rr.Order)
	Finish[rr.Order] = true // Confirm receipt
	return nil
}

// Collect remaining tasks
func (con *Connection) GetRest(rr *ReturnRestMaster, resp *string) error {
	waitingqueue = append(waitingqueue, *rr)
	*resp = "Received remaining tasks from " + strconv.Itoa(rr.Order)
	fmt.Println("Collected remaining tasks from", rr.Order, ", length: ", len(waitingqueue))
	return nil
}

// Communicate with workers
func connectionWithWorker() {
	// Register functionality object
	con := new(Connection)
	err := rpc.Register(con) // Custom service name rpc
	if err != nil {
		panic(err.Error())
	}
	// HTTP registration
	rpc.HandleHTTP()

	// Port listening
	listen, err := net.Listen("tcp", ":9090")
	if err != nil {
		panic(err.Error())
	}
	// Start service
	_ = http.Serve(listen, nil)
}

// Read the file line by line and store it in slice s
func GetLine(arraytemp *[]string) {

	// Open file
	file, err := os.Open("sample/sample3.txt")
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	defer file.Close()

	// Create scanner object for line-by-line scanning of file
	scanner := bufio.NewScanner(file)

	// Read file line by line and store
	for scanner.Scan() {
		*arraytemp = append(*arraytemp, scanner.Text())
		*arraytemp = append(*arraytemp, "\n ")
	}

	// Check for any errors
	if err = scanner.Err(); err != nil {
		fmt.Println("Error scanning file:", err)
	}
}

// Merge slice s into groups based on worker count, set to 10
func Grouping(s []string) {

	common := len(s) / 9 // Number of lines per group, 10 for overflow
	// Grouping, storing in sgroup
	for i := 0; i < len(s); i++ {
		sgroup[i/common] += s[i]
	}
}

// Summarize results collected from workers
func ShuffleAndReduce() {

	for {
		if numberofresults == 10 {
			SaveIntermediateResult()
			for i := 0; i < 10; i++ {
				for key, value := range results[i] {
					finalresult[key] += value
				}
			}
			fmt.Println("Summary successful")
			SaveFinalResult()
			break
		}
		// Current thread sleeps for one second
		time.Sleep(time.Second)
	}
}

// Save intermediate results
func SaveIntermediateResult() {
	fmt.Println("All results collected")
	fmt.Println("Writing intermediate result file")
	for i := 0; i < 10; i++ {
		temp := "intermediateresult/worker"
		temp += strconv.Itoa(i)
		temp += ".txt"
		filePath := temp
		file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			fmt.Println("Failed to open file", err)
		}
		// Close file handle in time
		defer file.Close()
		// Write to file using buffered *Writer
		write := bufio.NewWriter(file)
		for key, value := range results[i] {
			write.WriteString(key + ":" + strconv.Itoa(value) + "\n")
		}
		// Flush to truly write the buffered data to file
		write.Flush()
	}
}

// Save final results
func SaveFinalResult() {
	// Write to file
	temp := "finalresult/finalresult.txt"
	filePath := temp
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Failed to open file", err)
	}
	// Close file handle in time
	defer file.Close()
	// Write to file using buffered *Writer
	write := bufio.NewWriter(file)
	for key, value := range finalresult {
		write.WriteString(key + ":" + strconv.Itoa(value) + "\n")
	}
	// Flush to truly write the buffered data to file
	write.Flush()
}

// Monitor worker i's working time
func TimeLisener(order int) {
	for {
		if (StartTime[order] != 0) && (!Finish[order]) { // Task has started and result not submitted, work time should not exceed 10 seconds
			tcurrent := time.Now().UnixMicro() // Get current time

			if tcurrent-StartTime[order] > TIMELIMIT {
				fmt.Println(order, " has timed out")
				SetOverTime(order)
				break
			}
		}
		time.Sleep(time.Microsecond)
	}
}

// Set whether worker i has timed out
func SetOverTime(order int) {
	for {
		// Establish connection
		client, err := rpc.DialHTTP("tcp", "127.0.0.1:8081")
		if err != nil {
			panic(err.Error())
			// fmt.Println("Failed to connect to", order)
			time.Sleep(time.Microsecond)
			continue
		}

		var temp string
		// Synchronous call
		err = client.Call("Monitor.ConWithMaster", order, &temp)
		if err != nil {
			panic(err.Error())
		}
		fmt.Println(order, "work result:", temp)
		client.Close()
		break
	}
}

// Handover remaining tasks
func AssignRest() {
	for {
		if len(waitingqueue) > 0 { // Waiting queue for redistributing tasks is not empty
			// Distribute tasks
			for {
				// Establish connection
				client, err := rpc.DialHTTP("tcp", "127.0.0.1:8081")
				if err != nil {
					panic(err.Error())
					// fmt.Println("Failed to connect to", order)
					time.Sleep(time.Microsecond)
					continue
				}

				var temp string
				// Synchronous call
				err = client.Call("Monitor.ReceiveRest", waitingqueue[0], &temp)
				if err != nil {
					panic(err.Error())
				}
				fmt.Println(temp)
				fmt.Println("Assigned remaining tasks from", waitingqueue[0].Order)
				// Remove from queue
				waitingqueue = waitingqueue[1:]
				client.Close()
				break
			}
		}
		time.Sleep(time.Second)
	}
}

// Convert string to map
func JsonToMap(str string) map[string]int {

	var tempMap map[string]int

	err := json.Unmarshal([]byte(str), &tempMap)

	if err != nil {
		panic(err)
	}

	return tempMap
}

// Slice for grouped data
var sgroup []string

// Number of results received from workers
var numberofresults int

// Slice for results from each worker
var results []map[string]int

// Final result set
var finalresult map[string]int

// Start time collection for workers
var StartTime []int64

// Completion status queue for workers
var Finish []bool

// Waiting queue to store pending tasks
var waitingqueue []ReturnRestMaster

// Time limit for each worker's work
const TIMELIMIT int64 = 10000

func main() {

	// Initialize completion queue for workers
	Finish = []bool{false, false, false, false, false, false, false, false, false, false}
	// Initialize start time queue
	StartTime = make([]int64, 10)
	for i := 0; i < 10; i++ {
		StartTime[i] = 0
	}
	results = make([]map[string]int, 10)
	// Listen for remaining task queue
	go AssignRest()
	// Start monitoring each worker's working time
	for i := 0; i < 10; i++ {
		go TimeLisener(i)
	}
	// Initialize number of results
	numberofresults = 0
	// Slice for line-by-line reading
	var sline []string
	sgroup = make([]string, 10)        // Initialize slice for 10 groups
	finalresult = make(map[string]int) // Initialize final result set
	GetLine(&sline)                    // Read file line by line
	Grouping(sline)                    // Grouping
	go ShuffleAndReduce()              // Start thread to listen for all workers' results and summarize
	connectionWithWorker()             // Communicate with workers
}
