package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

type TaskInfo struct {
	file  string
	index int
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	const doWorker = "Worker.DoTask"
	var mutex sync.Mutex
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// wait group
	var wg sync.WaitGroup
	waitingWorker := make(chan string, 1024)
	finished := make(chan struct{}, 2)

	// init work task
	var workerTask = make(chan TaskInfo, 1024)
	go func() {
		mutex.Lock()
		switch phase {
		case mapPhase:
			for index, file := range mapFiles {
				workerTask <- TaskInfo{
					file:  file,
					index: index,
				}
			}
		case reducePhase:
			for i := 0; i < ntasks; i = i + 1 {
				workerTask <- TaskInfo{
					file:  "",
					index: i,
				}
			}
		default:
			fmt.Println(phase)
		}
		mutex.Unlock()
	}()

	// collection worker
	go func() {
		for {
			select {
			case worker := <-registerChan:
				waitingWorker <- worker
			case _ = <-finished:
				return
			}
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second)
			wg.Wait()
			mutex.Lock()
			if len(workerTask) == 0 {
				mutex.Unlock()
				finished <- struct{}{}
				finished <- struct{}{}
			} else {
				fmt.Printf("%d %d\n",len(workerTask),len(waitingWorker))
				mutex.Unlock()
			}
		}
	}()

	for {
		select {
		case file := <-workerTask:
			worker := <-waitingWorker
			var task DoTaskArgs
			task.File = file.file
			task.JobName = jobName
			task.NumOtherPhase = n_other
			task.Phase = phase
			task.TaskNumber = file.index
			wg.Add(1)
			go func(info DoTaskArgs, worker string) {
				defer wg.Done()
				ret := call(worker, doWorker, task, nil)
				if !ret {
					workerTask <- TaskInfo{
						file:  task.File,
						index: task.TaskNumber,
					}
				}
				waitingWorker <- worker
			}(task, worker)
		case _ = <-finished:
			return
		}

	}

}
