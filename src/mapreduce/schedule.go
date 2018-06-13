package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go mr.handleWork(phase, i, nios, &wg)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master) handleWork(phase jobPhase, taskNumber, numOtherPhase int, wg *sync.WaitGroup) {
	worker := <-mr.registerChannel
	args := &DoTaskArgs{
		JobName:       mr.jobName,
		File:          mr.files[taskNumber],
		Phase:         phase,
		TaskNumber:    taskNumber,
		NumOtherPhase: numOtherPhase,
	}
	reply := &struct{}{}
	if ok := call(worker, "Worker.DoTask", args, reply); !ok {
		go mr.handleWork(phase, taskNumber, numOtherPhase, wg)
	} else {
		// 这两个的顺序很重要
		wg.Done()
		mr.registerChannel <- worker
	}
}
