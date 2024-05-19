package mapreduce

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Worker holds the state for a server waiting for RunTask or Shutdown RPCs
type Worker struct {
	sync.Mutex

	name   string
	Map    func(string, string) []KeyValue
	Reduce func(string, []string) string
	nRPC   int // protected by mutex
	nTasks int // protected by mutex
	l      net.Listener

	shutdownChan     chan int
	shutdownOnSignal bool
}

// RunTask is called by the master when a new task is being scheduled on this
// worker.
func (wk *Worker) RunTask(arg *RunTaskArgs, _ *struct{}) error {
	debug("%s: given %v task #%d on file %s (numOtherPhase: %d)\n",
		wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	switch arg.Phase {
	case mapPhase:
		runMapTask(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case reducePhase:
		runReduceTask(arg.JobName, arg.TaskNumber, arg.NumOtherPhase, wk.Reduce)
	}

	debug("%s: %v task #%d done\n", wk.name, arg.Phase, arg.TaskNumber)
	return nil
}

// Shutdown is called by the master when all work has been completed.
// We should respond with the number of tasks we have processed.
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	debug("Shutdown %s\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.Ntasks = wk.nTasks
	wk.nRPC = 1
	wk.nTasks-- // Don't count the shutdown RPC
	if wk.shutdownOnSignal {
		wk.shutdownChan <- 1
	}
	return nil
}

// Tell the master we exist and ready to work
func (wk *Worker) register(master string) {
	args := new(RegisterArgs)
	args.Worker = wk.name
	ok := call(master, "Master.Register", args, new(struct{}))
	if !ok {
		log.Fatalf("Register: RPC %s register error\n", master)
	}
}

// RunWorker sets up a connection with the master, registers its address, and
// waits for tasks to be scheduled.
func RunWorker(MasterAddress string, me string,
	MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string,
	nRPC int, // Limit on RPC calls that can be invoked on the worker (-1 means no limit)
	shutdownOnSignal bool, // Should be True when running worker as an independent process
) {
	debug("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	wk.shutdownOnSignal = shutdownOnSignal
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	wk.register(MasterAddress)

	if shutdownOnSignal {
		wk.shutdownChan = make(chan int)
		go func() { // This thread is added to shut down the worker process after receiving a shutdown signal
			sig := <-wk.shutdownChan
			if sig == 1 {
				time.Sleep(time.Second)
				os.Exit(0)
			}
		}()
	}

	// DON'T MODIFY CODE BELOW
	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
			wk.Lock()
			wk.nTasks++
			wk.Unlock()
		} else {
			break
		}
	}
	wk.l.Close()
	debug("RunWorker %s exit\n", me)
}
