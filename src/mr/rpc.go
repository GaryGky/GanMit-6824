package mr

import "os"
import "strconv"

// Add your RPC definitions here.

type GetTaskArgs struct{}

type GetTaskReply struct {
	NReducer int
	WorkerID int32
	TaskPipe chan *Task
}

type NotifyTaskArgs struct {
	TaskID int
}

type NotifyTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
