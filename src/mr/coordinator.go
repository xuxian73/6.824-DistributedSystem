package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
import "time"

const (
	Ready		=	0
	Queue		=	1
	Running		=	2
	Finished	=	3
)

const (
	mapTask		=	0
	reduceTask	=	1
)

const (
	reschedule = time.Second * 5
)

type TaskStat struct {
	stat int
	workerid int
	startTime time.Time
}

type Task struct {
	Filename string
	Phase int
	Valid int
	Active int
	NReduce int
	NMap int
	Slot int
}

type Coordinator struct {
	// Your definitions here.
	mu 			sync.Mutex
	workerNum 	int
	done		bool
	nReduce		int
	nMap 		int
	filename 	[]string
	phase		int
	taskStat	[]TaskStat
	taskChan	chan Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReqTask (args *ReqTaskArgs, reply *ReqTaskReply) error{
	c.mu.Lock()
	
	flag := false
	if c.done {
		reply.Task.Active = 0
		c.mu.Unlock()
		return nil
	}
	for i,v := range c.taskStat{
		if v.stat == Ready {
			reply.Task = c.getTask(i)
			flag = true
			break
		}

	}
	if !flag {
		reply.Task.Valid = 0
		reply.Task.Active = 1
		c.mu.Unlock()
		return nil
	}
	i := reply.Task.Slot
	c.taskStat[i].workerid = args.Workerid
	c.taskStat[i].stat = Running
	c.taskStat[i].startTime = time.Now() 
	fmt.Printf("ReqTask: %d get %d task at slot %d\n", args.Workerid, c.phase, reply.Task.Slot)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error{
	c.mu.Lock()
	if c.taskStat[args.Slot].stat == Running && c.taskStat[args.Slot].workerid == args.Workerid && args.Err == 0 {
		fmt.Printf("ReportTask: %d report %d task at slot %d\n", args.Workerid, c.phase, args.Slot)
		c.taskStat[args.Slot].stat = Finished
	} else {
		c.taskStat[args.Slot].stat = Ready
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) WorkerReg(args *WorkerRegArgs, reply *WorkerRegReply) error {
	c.mu.Lock()
	reply.Id = c.workerNum
	c.workerNum += 1
	c.mu.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// scheduler
//
func (c *Coordinator)getTask(ind int) Task{
	task := Task{
		Filename: "",
		Phase: c.phase,
		Valid: 1,
		Active: 1,
		NReduce: c.nReduce,
		NMap: c.nMap,
		Slot: ind,
	}
	if ind < len(c.filename){
		task.Filename = c.filename[ind]
	}
	return task
}

func (c *Coordinator)Check() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return
	}
	
		done := true
		for i, v := range c.taskStat {
			switch v.stat {
			case Ready:
				done = false
			case Queue:
				done = false
			case Running:
				done = false
				if time.Since(c.taskStat[i].startTime) > reschedule {
					c.taskStat[i].stat = Ready
				}
			case Finished:
			default:
				fmt.Printf("Undefined stat")
			}
		}
		if done {
			if c.phase == mapTask {
				c.phase = reduceTask
				for i := range c.taskStat {
					if i < c.nReduce {
						c.taskStat[i].stat = Ready;
					}
				}
			} else {
				c.done = true
			}
		}
	
}

func (c *Coordinator)tickCheck(){
	for {
		if(!c.Done()){
			fmt.Printf("tickCheck...\n")
			
			c.Check()
		}
		time.Sleep(time.Second)
	}
}
//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock();
	defer c.mu.Unlock()

	// Your code here.
	return c.done
}

func (c *Coordinator) initMapTask() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.taskStat {
		if i < len(c.filename) {
			c.taskStat[i].stat = Ready;
		} else {
			c.taskStat[i].stat = Finished
		}
		//fmt.Printf("%d\n", c.taskStat[i].stat)
	}
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mu = sync.Mutex{}
	c.done = false
	c.workerNum = 1
	c.filename = files;
	c.phase = mapTask
	c.nReduce = nReduce
	c.nMap = len(files)
	size := len(files)
	if size < nReduce {
		size = nReduce
	}
	c.taskStat = make([]TaskStat, size)
	c.taskChan = make(chan Task, size)
	c.initMapTask();
	go c.tickCheck()
	fmt.Printf("Coordinator starting to server.\n");
	c.server()
	return &c
}
