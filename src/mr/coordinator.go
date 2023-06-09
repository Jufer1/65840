package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	mapPhase = iota
	reducePhase
	finishPhase
)

type Coordinator struct {
	// Your definitions here.
	nJob          int
	filename      []string
	nReduce       int
	mu            sync.Mutex
	phase         int
	cJobAllocated []bool
	cJAStamp      []int64
	cJobDone      []bool
}

func (c *Coordinator) getJobIndex() int {
	for i := 1; i < len(c.cJobAllocated); i++ {
		if !c.cJobAllocated[i] {
			return i
		}
	}
	return 0
}

func (c *Coordinator) checkALlJobDone() int {
	for i := 1; i < len(c.cJobDone); i++ {
		if !c.cJobDone[i] {
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
				if c.checkALlJobDone() == 0 {
					c.phase++
				} else {
					response.TType = WaitJob
				}
			} else {
				response.TType = MapJob
				response.Filename = c.filename[jobIndex-1]
				response.TaskIndex = jobIndex
				c.cJobAllocated[jobIndex] = true
				c.cJAStamp[jobIndex] = time.Now().Unix()
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

func (c *Coordinator) jobDaemon() {
	go func() {
		for {
			nowST := time.Now().Unix()
			c.mu.Lock()
			for i := 1; i < len(c.cJAStamp); i++ {
				st := c.cJAStamp[i]
				if st != 0 && nowST-st > 10 {
					c.cJobAllocated[i] = false
				}
			}
			c.mu.Unlock()
			time.Sleep(time.Second * 2)
		}
	}()
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
	c.cJobAllocated = make([]bool, len(files)+1)
	c.cJobDone = make([]bool, len(files)+1)
	c.cJAStamp = make([]int64, len(files)+1)
	// Your code here.
	c.jobDaemon()
	c.server()
	return &c
}
