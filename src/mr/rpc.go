package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type RequestArgs struct {
	ID int
}

type ExampleReply struct {
	Y int
}

const (
	Waiting    = iota
	MapTask    = iota
	ReduceTask = iota
)

const (
	// SUCCESS rpc success
	SUCCESS = iota
)

type RpcError struct {
	msg string
}

func (err *RpcError) Error() string {
	return err.msg
}

type ErrorCode int

type ResponseArgs struct {
	Errorcode ErrorCode
}

type GetTaskReply struct {
	ErrorCode       ErrorCode
	ID              int
	TaskType        int
	MapTaskFilename string
	ReduceNum       int
	MapTaskNum      int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
