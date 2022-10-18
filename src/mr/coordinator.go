package mr

import (
	"fmt"
	"log"
	"os"
	rDebug "runtime/debug"
	"sync"
	"time"

	"6.824/debug"
)
import "net"
import "net/rpc"
import "net/http"

type Progress int
type Phase int
type SyncMap sync.Map

const (
	Initiate   Progress = 1
	Processing Progress = 2
	Finish     Progress = 3
)

const (
	PhaseMap    Phase = 1
	PhaseReduce Phase = 2
	PhaseDone   Phase = 3
)

type Task struct {
	ID         int      // taskID auto-increment
	FileNames  []string // input file for the worker to read
	Progress   Progress // current task progress
	Phase      Phase    // notes the task is whether map or reduce
	CreateTime time.Time
}

type Coordinator struct {
	WorkerCnt       int32 // worker count
	NReducer        int
	TaskPipe        chan *Task
	Phase           Phase    // Track MapReduce Phase
	ProcessingTasks sync.Map // Track Processing Tasks
	Files           []string // all the input files
	Lock            sync.Mutex
}

func (c *Coordinator) HandleWorkerRegister(_ *WorkerRegisterArgs, reply *WorkerRegisterReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	reply.WorkerID = c.WorkerCnt
	reply.NReducer = c.NReducer
	c.WorkerCnt++
	return nil
}

func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := <-c.TaskPipe
	if task == nil {
		return nil
	}
	reply.SingleTask = task
	debug.Debug(debug.DInfo, "coordinator.HandleGetTask: return task-%d to worker-%d \n", reply.SingleTask.ID, args.WorkerID)
	return nil
}

func (c *Coordinator) HandleNotifyTask(args *NotifyTaskArgs, _ *NotifyTaskReply) error {
	debug.Debug(debug.DInfo, "coordinator.HandleNotifyTask: %v\n", args)

	c.ProcessingTasks.Delete(args.TaskID)
	if (*SyncMap)(&c.ProcessingTasks).isEmpty() {
		c.Lock.Lock()
		defer c.Lock.Unlock()
		switch c.Phase {
		case PhaseMap:
			c.generateReduceTasks()
			c.Phase = PhaseReduce
		case PhaseReduce:
			c.Phase = PhaseDone
			close(c.TaskPipe)
		}
	}
	return nil
}

func (sm *SyncMap) isEmpty() bool {
	cnt := 0
	(*sync.Map)(sm).Range(func(key, value interface{}) bool {
		cnt++
		return true
	})
	return cnt == 0
}

// Done main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	return c.Phase == PhaseDone
}

// MakeCoordinator create a Coordinator. main/mrcoordinator.go calls this function.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Initialize(files, nReduce)
	debug.Debug(debug.DInfo, "Coordinator is ready to serve... \n")
	c.server()
	return &c
}

func (c *Coordinator) Initialize(files []string, nReduce int) {
	c.NReducer = nReduce
	c.Files = files
	c.TaskPipe = make(chan *Task, len(files))
	c.WorkerCnt = 0
	c.Phase = PhaseMap
	c.ProcessingTasks = sync.Map{}
	c.Lock = sync.Mutex{}
	c.generateMapTasks(files)
}

func (c *Coordinator) generateMapTasks(files []string) {
	id := 1
	for _, file := range files {
		task := &Task{
			ID:        id,
			FileNames: []string{file},
			Progress:  Initiate,
			Phase:     PhaseMap,
		}
		safeGo(func() {
			c.TaskPipe <- task
			debug.Debug(debug.DInfo, "Task-%d is put to channel, now has %d tasks \n", task.ID, len(c.TaskPipe))
		})
		c.ProcessingTasks.Store(task.ID, task)
		id++
	}
}

func (c *Coordinator) generateReduceTasks() {
	rID2Files := make(map[int][]string)
	debug.Debug(debug.DInfo, "Coordinator.generateReduceTasks: NReducer: %d, length of files: %d \n", c.NReducer, len(c.Files))
	for i := 0; i < c.NReducer; i++ {
		for j := 1; j <= len(c.Files); j++ {
			if j == 1 {
				rID2Files[i] = make([]string, 0)
			}
			rID2Files[i] = append(rID2Files[i], fmt.Sprintf("mr-%d-%d", j, i))
			debug.Debug(debug.DInfo, "Coordinator.generateReduceTasks: %v \n", rID2Files[i])
		}
	}

	for reducerID, files := range rID2Files {
		task := &Task{
			ID:         reducerID,
			FileNames:  files,
			Progress:   Initiate,
			Phase:      PhaseReduce,
			CreateTime: time.Now(),
		}
		safeGo(func() {
			c.TaskPipe <- task
		})
		debug.Debug(debug.DInfo, "Task-%d: created, channel length now is:%d \n", task.ID, len(c.TaskPipe))
		c.ProcessingTasks.Store(task.ID, task)
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, http.DefaultServeMux)
}

func RecoverAndLog() {
	if err := recover(); err != nil {
		stack := string(rDebug.Stack())
		debug.Debug(debug.DError, "%v \n", stack)
	}
}

func safeGo(fun func()) {
	defer RecoverAndLog()
	fun()
}
