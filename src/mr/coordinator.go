package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Progress int
type Phase int

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
	WorkerCnt       int // worker count
	NReducer        int
	TaskPipe        chan *Task
	Phase           Phase         // Track MapReduce Phase
	ProcessingTasks map[int]*Task // Track Processing Tasks
	Files           []string      // all the input files
}

func (c *Coordinator) HandleGetTask(_ *GetTaskArgs, reply *GetTaskReply) error {
	reply.TaskPipe = c.TaskPipe
	reply.WorkerID = c.WorkerCnt
	c.WorkerCnt++
	return nil
}

func (c *Coordinator) HandleNotifyTask(args *NotifyTaskArgs, _ *NotifyTaskReply) error {
	delete(c.ProcessingTasks, args.TaskID)
	if len(c.ProcessingTasks) == 0 {
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

// Done main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.Phase == PhaseDone
}

// MakeCoordinator create a Coordinator. main/mrcoordinator.go calls this function.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.NReducer = nReduce
	c.TaskPipe = make(chan *Task)
	c.WorkerCnt = 0
	c.Phase = PhaseMap
	c.generateMapTasks(files)

	c.server()
	return &c
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
		c.TaskPipe <- task
		c.ProcessingTasks[task.ID] = task
		id++
	}
}

func (c *Coordinator) generateReduceTasks() {
	rID2Files := make(map[int][]string)
	for i := 0; i < c.NReducer; i++ {
		for j := 0; j < len(c.Files); j++ {
			if j == 0 {
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
			CreateTime: time.Now(),
		}
		c.TaskPipe <- task
		c.ProcessingTasks[task.ID] = task
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.RemoveAll(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
