package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	mapPhase = iota
	reducePhase
	finishPhase
)

type Coordinator struct {
	// Your definitions here.
	nJob     int
	filename []string
	nReduce  int
	mu       sync.Mutex
	phase    int
	cJob     []bool
	cJobDone []bool
}

func (c *Coordinator) getJobIndex() int {
	for i := 1; i < len(c.cJob); i++ {
		if !c.cJob[i] {
			return i
		}
	}
	return 0
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Heartbeat(report *Report, response *Response) error {
	c.mu.Lock()
	// fmt.Printf("\"in\": %v\n", "in")
	defer c.mu.Unlock()
	if report.WJob != 0 {
		func() {
			c.cJobDone[report.WJob] = true
			report.WJob = 0
		}()
	}
	switch c.phase {
	case mapPhase:
		func() {
			if jobIndex := c.getJobIndex(); jobIndex == 0 {
				c.phase++
			} else {
				response.TType = MapJob
				response.Filename = c.filename[jobIndex-1]
				response.TaskIndex = jobIndex
				c.cJob[jobIndex] = true
			}
			fmt.Printf("c.phase: %v\n", c.phase)
		}()
	case reducePhase:
		func() {
			response.TType = WaitJob
			fmt.Printf("\"not implement\": %v\n", "not implement")
		}()
	case finishPhase:
		response.TType = CompleteJob
	}
	// fmt.Printf("\"out\": %v\n", "out")
	return nil

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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nJob = len(files)
	c.filename = files
	c.nReduce = nReduce
	c.phase = mapPhase
	c.cJob = make([]bool, len(files)+1)
	c.cJobDone = make([]bool, len(files)+1)
	// Your code here.

	c.server()
	return &c
}
