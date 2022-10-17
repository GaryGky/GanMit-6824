package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"6.824/debug"
)
import "log"
import "net/rpc"
import "hash/fnv"

var (
	_errRPCFailed = errors.New("RPC Failed")
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerImpl struct {
	NReducer   int
	TaskPipe   chan *Task
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
}

// Worker
// main/worker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	const errCtx = "mr.Worker"
	r, err := taskQuery()
	if err != nil {
		debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
		panic(err)
	}

	impl := &WorkerImpl{
		NReducer:   r.NReducer,
		TaskPipe:   r.TaskPipe,
		MapFunc:    mapf,
		ReduceFunc: reducef,
	}
	impl.mainProcessor()
}
func (w *WorkerImpl) mainProcessor() {
	const errCtx = "WorkerImpl mainProcessor"
	for task := range w.TaskPipe {
		task.CreateTime = time.Now()
		switch task.Phase {
		case PhaseMap:
			if err := w.processMap(task); err != nil {
				debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
			}
			if err := taskNotify(NotifyTaskArgs{TaskID: task.ID}); err != nil {
				debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
			}
			debug.Debug(debug.DInfo, "Task-%d, Phase:%v has been done \n", task.ID, task.Phase)
		case PhaseReduce:
			if err := w.processReduce(task); err != nil {
				debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
			}
			if err := taskNotify(NotifyTaskArgs{TaskID: task.ID}); err != nil {
				debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
			}
			debug.Debug(debug.DInfo, "Task-%d, Phase:%v has been done \n", task.ID, task.Phase)
		}
	}
}

func (w *WorkerImpl) processMap(task *Task) error {
	const errCtx = "mr.processMap"
	intermediate := make([]KeyValue, 0)
	filename := task.FileName[0]
	file, err := os.Open(filename)
	if err != nil {
		debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
	}
	file.Close()
	kva := w.MapFunc(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))
	rID2Filename := generateReduceFile(w.NReducer, task.ID)
	for i := 0; i < len(intermediate); {
		hashKey := ihash(intermediate[i].Key) % w.NReducer
		fmt.Fprintf(rID2Filename[hashKey], "%v %v\n", intermediate[i].Key, intermediate[i].Value)
	}
	return nil
}

func (w *WorkerImpl) processReduce(task *Task) error {
	return nil
}

// RPC Call on Worker Init: Get task channel from coordinator
func taskQuery() (GetTaskReply, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.HandleGetTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return reply, _errRPCFailed
	}
}

func taskNotify(args NotifyTaskArgs) error {
	if ok := call("Coordinator.HandleNotifyTask", &args, &NotifyTaskReply{}); !ok {
		return _errRPCFailed
	}
	return nil
}

// return mapping: reducerID -> intermediate file
func generateReduceFile(nReducer int, taskID int) map[int]*os.File {
	rID2Filename := make(map[int]*os.File)
	for i := 0; i < nReducer; i++ {
		rID2Filename[i], _ = os.Create(fmt.Sprintf("mr-%d-%d", taskID, i))
	}
	return rID2Filename
}

// send an RPC request to the coordinator, wait for the response, usually returns true. Returns false if something goes wrong.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
