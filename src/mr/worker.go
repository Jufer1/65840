package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapJob(filename string, mapf func(string, string) []KeyValue, jobIndex int) []InterFilename {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))

	interFilename := []InterFilename{}
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		filename = "mr-" + strconv.Itoa(jobIndex) + "-" + strconv.Itoa(ihash(kva[i].Key))
		interFilename = append(interFilename, InterFilename{M: jobIndex, R: ihash(kva[i].Key)})
		file, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		enc := json.NewEncoder(file)
		for k := i; k < j; k++ {
			if err := enc.Encode(&kva[k]); err != nil {
				fmt.Printf("\"fail to write json\": %v\n", "fail to write json")
			}
		}
		file.Close()

		i = j
	}
	return interFilename
}

func doReduceJob(filenames []string, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		// fmt.Printf("doReduceJob kva: %v\n", kva)
		// time.Sleep(time.Second * 5)
	}

	values := []string{}
	for _, v := range kva {
		values = append(values, v.Value)
	}

	output := reducef(kva[0].Key, values)

	fmt.Println("key ", kva[0].Key, " value ", output)
	// this is the correct format for each line of Reduce output.
	// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	report := Report{}
	response := Response{}
	for {
		// fmt.Printf("\"iam here before heartbeat\": %v\n", "iam here")
		heartbeat(&report, &response)
		// fmt.Printf("\"iam here after heartbeat\": %v\n", "iam here")
		switch response.TType {
		case MapJob:
			fmt.Printf("\"hit here MapJob\": %v\n", "hit here MapJob")
			// tmp := time.Now().Unix()
			// fmt.Printf("response: %v\n", response)
			// report.Filename = doMapJob(response.Filename, mapf, response.TaskIndex) //not need to report all mr- parameters
			doMapJob(response.Filename, mapf, response.TaskIndex)
			report.WJob = response.TaskIndex
			report.WJobType = response.TType
			// fmt.Println(response.TaskIndex, "cost:", time.Now().Unix()-tmp)
		case ReduceJob:
			doReduceJob(response.ReduceFilenames, reducef)
			report.WJob = response.TaskIndex
			report.WJobType = response.TType
			// time.Sleep(time.Second)
		case WaitJob:
			report.WJob = response.TaskIndex
			report.WJobType = response.TType
			fmt.Println("waiting for a job.")
			time.Sleep(time.Second)
		case CompleteJob:
			fmt.Printf("\"worker exit\": %v\n", "worker exit")
			return
		case TestJob:
			fmt.Println("---------TestJob---------.")
			time.Sleep(time.Second)
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.TType))
		}
		// time.Sleep(time.Second)
	}

}

func heartbeat(report *Report, response *Response) {
	// report := Report{}
	// response := Response{}

	ok := call("Coordinator.Heartbeat", &report, &response)
	if ok {
		fmt.Printf("response: %v\n", response)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
