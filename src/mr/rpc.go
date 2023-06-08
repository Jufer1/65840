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

const (
	MapJob = iota
	ReduceJob
	WaitJob
	CompleteJob
)

type InterFilename struct {
	M int
	R int
}

type Report struct {
	WJob     int
	WJobType int
	WJobDone bool
	Filename []InterFilename
}

type Response struct {
	TaskIndex int
	TType     int
	Filename  string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
