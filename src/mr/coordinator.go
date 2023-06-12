package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type ByR []tmpRFN

func (a ByR) Len() int           { return len(a) }
func (a ByR) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByR) Less(i, j int) bool { return a[i].r < a[j].r }

const (
	mapPhase = iota
	reducePhase
	finishPhase
	testPhase
)

type tmpRFN struct {
	m string
	r string
}

type reduceFilename struct {
	fns []string
}

type Coordinator struct {
	// Your definitions here.
	nJob            int
	filename        []string
	nReduce         int
	mu              sync.Mutex
	phase           int
	cJobAllocated   []bool
	cJAStamp        []int64
	cJobDone        []bool
	reduceFilenames []reduceFilename
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

func (c *Coordinator) preSwitchToReduce() {

	files, err := ioutil.ReadDir(".")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	filenames := []string{}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-") {
			filenames = append(filenames, file.Name())
		}
	}

	pattern := regexp.MustCompile(`mr-(\d+)-(\d+)`)

	tmprfns := []tmpRFN{}

	for _, v := range filenames {
		// fmt.Printf("filename: %v\n", v)
		matches := pattern.FindStringSubmatch(v)
		if len(matches) >= 2 {
			// fmt.Printf("第一个数字: %s\t", matches[1])
			// fmt.Printf("第二个数字: %s\n", matches[2])

			tmprfns = append(tmprfns, tmpRFN{m: matches[1], r: matches[2]})
		} else {
			fmt.Println("extract the number from mr- failed")
		}
	}
	sort.Sort(ByR(tmprfns))

	for i := 0; i < len(tmprfns); i++ {
		j := i + 1
		for j < len(tmprfns) && tmprfns[j].r == tmprfns[i].r {
			j++
		}
		tmpr := []tmpRFN{}
		for k := i; k < j; k++ {
			tmpr = append(tmpr, tmpRFN{tmprfns[k].m, tmprfns[k].r})
		}
		// fmt.Printf("i: %v\n", i)
		// fmt.Printf("tmpr: %v\n", tmpr)
		tmprfns2 := []string{}
		for _, v := range tmpr {
			tmprfn := "mr-" + v.m + "-" + v.r
			tmprfns2 = append(tmprfns2, tmprfn)
		}
		c.reduceFilenames = append(c.reduceFilenames, reduceFilename{fns: tmprfns2})
		// fmt.Printf("c.reduceFilenames: %v\n", c.reduceFilenames)
		i = j
		// time.Sleep(time.Second * 5)
	}

	c.phase = reducePhase
	c.nJob = len(c.reduceFilenames)
	c.cJobAllocated = make([]bool, len(c.reduceFilenames)+1)
	c.cJobDone = make([]bool, len(c.reduceFilenames)+1)
	c.cJAStamp = make([]int64, len(c.reduceFilenames)+1)
}

func (c *Coordinator) Heartbeat(report *Report, response *Response) error {
	fmt.Printf("report job: %v\n", report.WJob)
	c.mu.Lock()
	// fmt.Printf("\"in\": %v\n", "in")
	defer c.mu.Unlock()
	if report.WJob != 0 {
		func() {
			// fmt.Printf("report.WJob: %v\n", report.WJob)
			c.cJobDone[report.WJob] = true
			// fmt.Printf("before c.cJAStamp[%v]: %v\n", report.WJob, c.cJAStamp[report.WJob])
			c.cJAStamp[report.WJob] = 0 //time stamp = 0 avoid detected by daemon wrongly
			report.WJob = 0
			// fmt.Printf("after c.cJAStamp[%v]: %v\n", report.WJob, c.cJAStamp[report.WJob])
		}()
	}
	switch c.phase {
	case mapPhase:
		func() {
			if jobIndex := c.getJobIndex(); jobIndex == 0 {
				report.WJob = 0 //switch to reduce phase avoid job marked done but not even allocated
				if c.checkALlJobDone() == 0 {
					c.preSwitchToReduce()
				} else {
					fmt.Println("Wait for all mapjob done.")
					response.TType = WaitJob
				}
			} else {
				response.TType = MapJob
				response.Filename = c.filename[jobIndex-1]
				response.TaskIndex = jobIndex
				c.cJobAllocated[jobIndex] = true
				c.cJAStamp[jobIndex] = time.Now().Unix()
				// fmt.Printf("response: %v\n", response)
			}
		}()
	case reducePhase:
		func() {
			if jobIndex := c.getJobIndex(); jobIndex == 0 {
				report.WJob = 0 //switch to reduce phase avoid job marked done but not even allocated
				if c.checkALlJobDone() == 0 {
					c.phase++
				} else {
					fmt.Println("Wait for all reducejob done.")
					response.TType = WaitJob
				}
			} else {
				response.TType = ReduceJob
				response.ReduceFilenames = c.reduceFilenames[jobIndex-1].fns
				response.TaskIndex = jobIndex
				c.cJobAllocated[jobIndex] = true
				c.cJAStamp[jobIndex] = time.Now().Unix()
				// fmt.Printf("response: %v\n", response)
			}
		}()
	case finishPhase:
		response.TType = CompleteJob
	case testPhase:
		if time.Now().Unix()%2 == 0 {
			response.TType = WaitJob
		} else {
			response.TType = TestJob
		}
	}
	// fmt.Printf("\"out\": %v\n", "out")
	fmt.Printf("HEARTBEAT: c.phase: %v\n", c.phase)
	fmt.Printf("response: %v\n", response)
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
		// for {
		// 	time.Sleep(time.Second)
		// 	fmt.Printf("c.cJobAllocated: %v\n", c.cJobAllocated)
		// 	fmt.Printf("c.cJobDone: %v\n", c.cJobDone)
		// }
		for {
			c.mu.Lock()
			nowST := time.Now().Unix()
			for i := 1; i < len(c.cJAStamp); i++ {
				st := c.cJAStamp[i]
				if st != 0 && nowST-st > 30 {
					c.cJobAllocated[i] = false
					// fmt.Printf("st: %v\n", st)
					// fmt.Printf("c.cJobAllocated[i]: %v\n", c.cJobAllocated[i])
				}
			}
			c.mu.Unlock()
			time.Sleep(time.Second * 10)
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
	// c.phase = testPhase
	c.cJobAllocated = make([]bool, len(files)+1)
	c.cJobDone = make([]bool, len(files)+1)
	c.cJAStamp = make([]int64, len(files)+1)
	// Your code here.
	c.jobDaemon()
	c.server()
	return &c
}
