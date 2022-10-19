package mr

import (
	"encoding/json"
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

	_breakTime = time.Second * 5
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
	WorkerID   int32
	NReducer   int
	TaskPipe   chan *Task
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
}

// Worker
// main/worker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	r, err := rpcWorkerRegister()
	if err != nil {
		panic(fmt.Sprintf("worker init failed: %v \n", err))
	}
	impl := &WorkerImpl{
		NReducer:   r.NReducer,
		MapFunc:    mapf,
		ReduceFunc: reducef,
		WorkerID:   r.WorkerID,
	}
	impl.mainProcessor()
}
func (w *WorkerImpl) mainProcessor() {
	const errCtx = "mr.Worker.mainProcessor"
	for {
		// send rpc to query task
		r, err := rpcTaskQuery(int(w.WorkerID))
		if err != nil {
			debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
			return
		}
		if r.SingleTask.Phase == PhaseDone {
			debug.Debug(debug.DInfo, "worker-%d receive exit signal: %v \n", w.WorkerID, r.SingleTask.Phase)
			break
		}

		// set task status
		debug.Debug(debug.DInfo, "worker-%d receive task: %d \n", w.WorkerID, r.SingleTask.ID)
		task := r.SingleTask

		// process tasks based on task phase
		switch task.Phase {
		case PhaseMap:
			if err := w.processMap(task); err != nil {
				debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
			}
			task.Progress = Finish
			if err := rpcTaskNotify(NotifyTaskArgs{TaskID: task.ID}); err != nil {
				debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
			}
			debug.Debug(debug.DInfo, "Worker.mainProcess: Task-%d, Phase:%v has been done \n", task.ID, task.Phase)
		case PhaseReduce:
			if err := w.processReduce(task); err != nil {
				debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
			}
			task.Progress = Finish
			if err := rpcTaskNotify(NotifyTaskArgs{TaskID: task.ID}); err != nil {
				debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
			}
			debug.Debug(debug.DInfo, "Task-%d, Phase:%v has been done \n", task.ID, task.Phase)
		}
	}
}

func (w *WorkerImpl) processMap(task *Task) error {
	const errCtx = "mr.Worker.processMap"
	intermediate := make([]KeyValue, 0)
	filename := task.FileNames[0]
	file, err := os.Open(filename)
	if err != nil {
		debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		debug.Debug(debug.DError, "%v: %v \n", errCtx, err)
		return err
	}
	defer file.Close()
	kva := w.MapFunc(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))
	rID2FileEncoder := filenamesToFileEncoders(generateFilenames(w.NReducer, task.ID))
	for i := 0; i < len(intermediate); i++ {
		hashKey := ihash(intermediate[i].Key) % w.NReducer
		rID2FileEncoder[hashKey].Encode(&intermediate[i])
	}
	return nil
}

func (w *WorkerImpl) processReduce(task *Task) error {
	const errCtx = "mr.Worker.processMap"
	intermediate := make([]KeyValue, 0)
	decoders := filenamesToFileDecoders(task.FileNames)
	for _, dec := range decoders {
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	oName := fmt.Sprintf("mr-out-%d", task.ID)
	oFile, _ := os.Create(oName)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.ReduceFunc(intermediate[i].Key, values)
		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	return nil
}

// RPC Call on Worker Init: Get task channel from coordinator
func rpcTaskQuery(workerID int) (GetTaskReply, error) {
	args := GetTaskArgs{WorkerID: workerID}
	reply := GetTaskReply{}
	ok := call("Coordinator.HandleGetTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return reply, _errRPCFailed
	}
}

func rpcTaskNotify(args NotifyTaskArgs) error {
	if ok := call("Coordinator.HandleNotifyTask", &args, &NotifyTaskReply{}); !ok {
		return _errRPCFailed
	}
	return nil
}

func rpcWorkerRegister() (WorkerRegisterReply, error) {
	resp := WorkerRegisterReply{}
	if ok := call("Coordinator.HandleWorkerRegister", &WorkerRegisterArgs{}, &resp); !ok {
		return resp, _errRPCFailed
	}
	return resp, nil
}

// generate filenames based on nReducer and taskID
func generateFilenames(nReducer int, taskID int) []string {
	fileNames := make([]string, 0)
	for i := 0; i < nReducer; i++ {
		fileNames = append(fileNames, fmt.Sprintf("mr-%d-%d", taskID, i))
	}
	return fileNames
}

// generate reduceID -> file encoders
func filenamesToFileEncoders(filenames []string) map[int]*json.Encoder {
	rID2FileEncoder := make(map[int]*json.Encoder)
	for i, filename := range filenames {
		oFile, _ := os.Create(filename)
		rID2FileEncoder[i] = json.NewEncoder(oFile)
	}
	return rID2FileEncoder
}

func filenamesToFileDecoders(filenames []string) map[string]*json.Decoder {
	rID2FileEncoder := make(map[string]*json.Decoder)
	for _, filename := range filenames {
		oFile, _ := os.Open(filename)
		rID2FileEncoder[filename] = json.NewDecoder(oFile)
	}
	return rID2FileEncoder
}

// send an RPC request to the coordinator, wait for the response, usually returns true. Returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
