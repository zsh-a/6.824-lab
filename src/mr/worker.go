package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var Id = -1

func keepAlive() {
	args := RequestArgs{ID: Id}
	reply := ResponseArgs{}
	call("Master.KeepLive", &args, &reply)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	go func() {
		for {
			time.Sleep(5 * time.Second)
			keepAlive()
		}
	}()
	// Your worker implementation here.
	for {
		task := callGetTask()
		Id = task.ID
		switch task.TaskType {

		case Waiting:
			time.Sleep(2 * time.Second)
		case MapTask:
			doMap(task.MapTaskFilename, mapf, task.ReduceNum, task.MapTaskNum)
			for {
				err := complete()
				if err == nil {
					break
				}
				log.Println("worker id :", Id, "  ", err)
			}
		case ReduceTask:
			doReduce(task.ReduceNum, reducef, task.MapTaskNum)
			for {
				err := complete()
				if err == nil {
					break
				}
				log.Println("worker id :", Id, "  ", err)
			}
		}
	}
}

func complete() error {
	args := RequestArgs{ID: Id}
	reply := ResponseArgs{}
	call("Master.TaskComplete", &args, &reply)
	if reply.Errorcode == SUCCESS {
		return nil
	}
	return &RpcError{"no response"}
}

func doReduce(numReduce int, reducef func(string, []string) string, mapNum int) {
	outname := "mr-out-" + strconv.Itoa(numReduce)
	file, err := os.Create(outname)
	if err != nil {
		panic("create file failed : " + outname)
	}
	defer file.Close()

	data := make(map[string][]string)
	for i := 0; i < mapNum; i++ {
		name := getOutFilename(i, numReduce)
		file, err := os.Open(name)
		if err != nil {
			log.Fatal("Cannot open file : ", name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			data[kv.Key] = append(data[kv.Key], kv.Value)
		}
	}
	for key := range data {
		value := reducef(key, data[key])
		file.WriteString(fmt.Sprintf("%v %v\n", key, value))
	}
}

func doMap(filename string, mapf func(string, string) []KeyValue, nReduce, mapNum int) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		name := getOutFilename(mapNum, i)
		file, err := os.Create(name)
		if err != nil {
			panic("create out file failed")
		}
		defer file.Close()
		files[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	for i := 0; i < len(kva); i++ {
		slot := ihash(kva[i].Key) % nReduce
		encoders[slot].Encode(kva[i])
	}
}

// num output file num
func getOutFilename(id, num int) string {
	return "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(num)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func callGetTask() *GetTaskReply {

	args := RequestArgs{ID: Id}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	return &reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
