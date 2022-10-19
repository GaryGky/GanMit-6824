package mr

import (
	"fmt"
	"log"
	"os"
	rDebug "runtime/debug"
	"sync"
	"time"

	"6.824/debug"
	"github.com/go-co-op/gocron"
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
	WorkerCnt        int32 // worker count
	NReducer         int
	TaskPipe         chan *Task
	Phase            Phase    // Track MapReduce Phase
	ProcessingTasks  sync.Map // Track Processing Tasks: (Key: taskID, Value: Task)
	Files            []string // all the input files
	PhaseLock        sync.Mutex
	TimeoutCheckLock sync.RWMutex
}

func (c *Coordinator) HandleWorkerRegister(_ *WorkerRegisterArgs, reply *WorkerRegisterReply) error {
	c.PhaseLock.Lock()
	defer c.PhaseLock.Unlock()
	reply.WorkerID = c.WorkerCnt
	reply.NReducer = c.NReducer
	c.WorkerCnt++
	return nil
}

func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	debug.Debug(debug.DInfo, "coordinator.HandleGetTask: received request from worker-%d \n", args.WorkerID)
	task, ok := <-c.TaskPipe
	if !ok {
		// there are no more values to receive and the channel is closed
		reply.SingleTask = &Task{
			Phase: c.Phase,
		}
		return nil
	}

	c.TimeoutCheckLock.RLock()
	defer c.TimeoutCheckLock.RUnlock()

	reply.SingleTask = task
	task.CreateTime = time.Now()
	task.Progress = Processing
	debug.Debug(debug.DInfo, "coordinator.HandleGetTask: return task-%d to worker-%d \n", reply.SingleTask.ID, args.WorkerID)
	return nil
}

func (c *Coordinator) HandleNotifyTask(args *NotifyTaskArgs, _ *NotifyTaskReply) error {
	debug.Debug(debug.DInfo, "coordinator.HandleNotifyTask: task-%d is done\n", args.TaskID)

	c.ProcessingTasks.Delete(args.TaskID)
	if (*SyncMap)(&c.ProcessingTasks).isEmpty() {
		c.PhaseLock.Lock()
		defer c.PhaseLock.Unlock()
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
	c.PhaseLock.Lock()
	defer c.PhaseLock.Unlock()
	debug.Debug(debug.DInfo, "Coordinator. Phase: %v \n", c.Phase)
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
	c.PhaseLock = sync.Mutex{}
	c.TimeoutCheckLock = sync.RWMutex{}
	c.generateMapTasks(files)
}

func (c *Coordinator) generateMapTasks(files []string) {
	id := 1
	for _, file := range files {
		task := &Task{
			ID:         id,
			FileNames:  []string{file},
			Progress:   Initiate,
			Phase:      PhaseMap,
			CreateTime: time.Now().Add(time.Minute),
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
		}
	}

	for reducerID, files := range rID2Files {
		task := &Task{
			ID:         reducerID,
			FileNames:  files,
			Progress:   Initiate,
			Phase:      PhaseReduce,
			CreateTime: time.Now().Add(time.Minute),
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
	go safeGo(func() {
		http.Serve(l, http.DefaultServeMux)
	})
	go safeGo(func() {
		c.checkTimeoutTask() // 可能会退出不了
	})
}

// periodically check c.ProcessingTasks and assign timeout tasks to new worker
func (c *Coordinator) checkTimeoutTask() {
	s := gocron.NewScheduler(time.UTC)
	s.Every(10).Seconds().Do(func() {
		c.TimeoutCheckLock.Lock()
		defer c.TimeoutCheckLock.Unlock()
		c.ProcessingTasks.Range(func(key, value interface{}) bool {
			task := value.(*Task)
			if time.Since(task.CreateTime) >= time.Second*10 {
				c.TaskPipe <- task
				debug.Debug(debug.DWarn, "task:%d is timeout\n", task.ID)
			}
			return true
		})
	})
	s.StartAsync()
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
