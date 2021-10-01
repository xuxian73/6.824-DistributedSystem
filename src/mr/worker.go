package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	WorkerID int
	mapf func(string, string)[]KeyValue
	reducef func(string, []string) string
	task Task
}
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.WorkerReg()
	w.run()
}

//
// worker run
//
func (w *worker) run() {
	for ; ; {
		w.reqTask()
		if w.task.Active == 0 {
			fmt.Printf("worker is noted to quit\n")
			os.Exit(0)
		}
		if w.task.Valid == 0 {
			fmt.Printf("worker %d: temporately no task\n", w.WorkerID)
		} else {
			w.doTask()
		}
		time.Sleep(time.Second)
	}
}

//
// ReqTask
//
func (w *worker) reqTask() {
	args := ReqTaskArgs{}
	reply := ReqTaskReply{}
	args.Workerid = w.WorkerID
	if err := call("Coordinator.ReqTask", &args, &reply); !err {
		fmt.Printf("worker: reqtask error\n")
		return
	}
	w.task = reply.Task
}

//
// ReportTask
//
func (w *worker) reportTask(err int, slot int, phase int) {
	args := ReportTaskArgs{
		Workerid: w.WorkerID,
		Err: err,
		Slot: slot,
		Phase: phase,
	}
	reply := ReportTaskReply{}
	if err := call("Coordinator.ReportTask", &args, &reply); !err {
		fmt.Printf("worker: reporttask error\n")
	}
}

//
// do Task (including map and reduce)
//
func (w *worker) doTask() {
	
	switch w.task.Phase {
	case mapTask:
		w.doMapTask()
	case reduceTask:
		w.doReduceTask()
	default:
		w.reportTask(1, w.task.Slot, w.task.Phase)
	}
}

func (w *worker) doMapTask() {
	intermediate := [][]KeyValue{}
	for size := 0; size < w.task.NReduce; size++{
		intermediate = append(intermediate, []KeyValue{})
	}
	file, err := os.Open(w.task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", w.task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", w.task.Filename)
	}
	file.Close()
	
	kva := w.mapf(w.task.Filename, string(content))
	
	for _,v := range kva {
		ind := ihash(v.Key) % w.task.NReduce
		intermediate[ind] = append(intermediate[ind], v)
	}
	for i,_ := range intermediate{
		sort.Sort(ByKey(intermediate[i]))
	}
	for i,v := range intermediate {
		filename := "tmp_" + strconv.Itoa(w.task.Slot) + "_" + strconv.Itoa(i)
		f, err := os.Create(filename)
		if err != nil {
			fmt.Printf("create file failed")
		}
		enc := json.NewEncoder(f)
		for _, kv := range v {
			enc.Encode(&kv)
		}
		f.Close()
	}

	w.reportTask(0, w.task.Slot, w.task.Phase)

}

func (w *worker) doReduceTask() {
	filename := "mr-out-" + strconv.Itoa(w.task.Slot)
	f, err := os.Create(filename)
		if err != nil {
			fmt.Printf("create file failed")
	}
	kva :=[]KeyValue{}
	for i:=0; i < w.task.NMap; i++ {
		filename := "tmp_" + strconv.Itoa(i) + "_" + strconv.Itoa(w.task.Slot)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", w.task.Filename)
		}
		dec := json.NewDecoder(file)
  		for {
    		var kv KeyValue
    		if err := dec.Decode(&kv); err != nil {
      			break
    		}
    		kva = append(kva, kv)
  		}
	}
	sort.Sort(ByKey(kva))
	i:=0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)

		i = j
	}
	
	w.reportTask(0, w.task.Slot, w.task.Phase)
}
//
// register worker
// notify the coordinator this worker is available
//
func (w *worker) WorkerReg() {
	args := WorkerRegArgs{}
	reply := WorkerRegReply{}
	if err := call("Coordinator.WorkerReg", &args, &reply); !err {
		fmt.Printf("failed to register\n")
		return
	}
	w.WorkerID = reply.Id
	fmt.Printf("WorkerReg %d\n", w.WorkerID)
} 

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
