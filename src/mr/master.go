package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Idle       = iota
	InProgress = iota
	Completed  = iota
	Crashed    = iota
)

type WorkerState struct {
	TaskType  int
	State     int
	TaskIndex int
	Timeout   time.Time
}
type Master struct {
	// Your definitions here.
	WorkerNum              int
	Workers                map[int]*WorkerState
	Files                  []string
	FilesState             []int
	MapTaskCompletedNum    int
	MapTaskNum             int
	ReduceTaskNum          int
	ReduceTaskState        []int
	ReduceTaskCompletedNum int
	Mutex                  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

var completed = make(chan int)

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) GetTask(args *RequestArgs, reply *GetTaskReply) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if args.ID == -1 {
		reply.ID = m.WorkerNum
		m.WorkerNum++
		fmt.Println("id", reply.ID)
		m.Workers[reply.ID] = &WorkerState{State: Idle}
	} else {
		reply.ID = args.ID
	}
	reply.TaskType = Waiting
	if m.Done() {
		return nil
	}
	m.Workers[reply.ID].Timeout = time.Now()
	if m.MapTaskCompletedNum < m.MapTaskNum {
		for i := 0; i < len(m.Files); i++ {
			if m.FilesState[i] == Idle {
				reply.ErrorCode = SUCCESS
				reply.MapTaskFilename = m.Files[i]
				reply.TaskType = MapTask
				reply.MapTaskNum = i
				reply.ReduceNum = m.ReduceTaskNum
				m.Workers[reply.ID].TaskIndex = i
				m.FilesState[i] = InProgress
				m.Workers[reply.ID].TaskType = MapTask
				m.Workers[reply.ID].State = InProgress

				return nil
			}
		}
	} else {
		for i := 0; i < m.ReduceTaskNum; i++ {
			if m.ReduceTaskState[i] == Idle {
				reply.ErrorCode = SUCCESS
				reply.MapTaskNum = m.MapTaskNum
				reply.TaskType = ReduceTask
				reply.ReduceNum = i
				m.Workers[reply.ID].TaskIndex = i
				m.Workers[reply.ID].State = InProgress
				m.ReduceTaskState[i] = InProgress
				m.Workers[reply.ID].TaskType = ReduceTask
				return nil
			}
		}
	}
	return nil
}

func (m *Master) KeepLive(args *RequestArgs, reply *ResponseArgs) error {
	id := args.ID
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	fmt.Println("keep alive")
	m.Workers[id].Timeout = time.Now()
	return nil
}

func (m *Master) TaskComplete(args *RequestArgs, reply *ResponseArgs) error {
	id := args.ID
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	m.Workers[id].State = Idle
	if m.Workers[id].TaskType == MapTask {
		m.FilesState[m.Workers[id].TaskIndex] = Completed
		m.MapTaskCompletedNum++
	} else {
		m.ReduceTaskState[m.Workers[id].TaskIndex] = Completed
		m.ReduceTaskCompletedNum++
	}
	reply.Errorcode = SUCCESS
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	if m.ReduceTaskNum == m.ReduceTaskCompletedNum {
		return true
	}
	return ret
}

const (
	crashTimeout = 10
)

func (m *Master) crashDetect() {
	for {
		time.Sleep(10 * time.Second)
		for i := 0; i < m.WorkerNum; i++ {
			old := m.Workers[i].Timeout
			if m.Workers[i].State == InProgress && time.Since(old).Seconds() >= crashTimeout {
				m.Workers[i].State = Crashed
				if m.Workers[i].TaskType == MapTask {
					m.FilesState[m.Workers[i].TaskIndex] = Idle
				} else {
					m.ReduceTaskState[m.Workers[i].TaskIndex] = Idle
				}
			}
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.WorkerNum = 0
	m.Files = files
	m.MapTaskNum = len(files)
	// Your code here.
	m.MapTaskCompletedNum = 0
	m.ReduceTaskNum = nReduce
	m.FilesState = make([]int, m.MapTaskNum)
	m.ReduceTaskState = make([]int, nReduce)
	m.Workers = make(map[int]*WorkerState)
	m.ReduceTaskCompletedNum = 0

	for i := 0; i < m.MapTaskNum; i++ {
		m.FilesState[i] = Idle

	}
	for i := 0; i < nReduce; i++ {
		m.ReduceTaskState[i] = Idle
	}

	m.server()
	go m.crashDetect()
	return &m
}
