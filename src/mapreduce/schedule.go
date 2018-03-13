package mapreduce

import (
	"fmt"
	//"sync"
	//"log"
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

	// schedule will wait until all worker has done their jobs

	//因为master中有个存registerChannel的chan,chan是阻塞的
	//所以肯定要从那里面取registerChannel来用
	//调用Worker的DoTask函数

	//大致流程就是worker领任务，完成任务后，继续领任务

	task := new(DoTaskArgs)
	task.JobName = mr.jobName
	task.Phase = phase
	task.NumOtherPhase = nios
	worker := <- mr.registerChannel


	var reply interface{}
	fmt.Printf("one worker test %s\n", task.JobName)

	for i := 0; i < ntasks; i++ {
		task.TaskNumber = i
		if (phase == mapPhase) {
			task.File = mr.files[i]
		}
		call(worker, "Worker.DoTask", task, reply)
	}

	fmt.Printf("the for loop is over\n")
	fmt.Printf("worker is %v\n", worker)
	//重复利用worker
	go func() {
		mr.registerChannel <- worker
	}()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
