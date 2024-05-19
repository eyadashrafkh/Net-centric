package mapreduce

import (
    "sync"
)

func (mr *Master) schedule(phase jobPhase) {
    var ntasks int
    var numOtherPhase int
    switch phase {
    case mapPhase:
        ntasks = len(mr.files)     // number of map tasks
        numOtherPhase = mr.nReduce // number of reducers
    case reducePhase:
        ntasks = mr.nReduce           // number of reduce tasks
        numOtherPhase = len(mr.files) // number of map tasks
    }

    debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

    var wg sync.WaitGroup
    wg.Add(ntasks)
    for i := 0; i < ntasks; i++ {
        go func(taskNumber int) {
            for {
                worker := <-mr.registerChannel
                taskArgs := &RunTaskArgs{
                    JobName:       mr.jobName,
                    File:          mr.files[taskNumber],
                    Phase:         phase,
                    TaskNumber:    taskNumber,
                    NumOtherPhase: numOtherPhase,
                }
                success := call(worker, "RunTask", taskArgs, new(struct{}))
                if success {
                    mr.registerChannel <- worker
                    break
                } else {
                    debug("Task %v failed, reassigning to another worker\n", taskNumber)
                    continue
                }
            }
            wg.Done()
        }(i)
    }
    wg.Wait()

    debug("Schedule: %v phase done\n", phase)
}
