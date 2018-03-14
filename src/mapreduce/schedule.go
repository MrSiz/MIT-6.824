package mapreduce

import (
	"fmt"
	//"sync"
	//"log"
	//"sync"
	//"sync"
	//"unicode"
	"sync"
)


// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files) //map过程中对应打就是文件数
		nios = mr.nReduce //多少个reduce任务
	case reducePhase:
		ntasks = mr.nReduce //与上面的相反
		nios = len(mr.files)
	}

	//DoTaskArgs中的那个taskNumber就是具体的索引
	//从worker的执行代码逆推

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

	//以下代码是单个worker能够顺利工作的
	/*task := new(DoTaskArgs)
	task.JobName = mr.jobName
	task.Phase = phase
	task.NumOtherPhase = nios
	worker := <- mr.registerChannel //获取一个worker


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

	go func() {
		mr.registerChannel <- worker	//重复利用worker
	}()*/




	//多个worker同时工作，那最主要的问题就是解决同步问题了
	//参考https://studygolang.com/articles/2027

	////使用等待队列来等待所有的worker完成工作
	var waitgroup sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		//需要的等待工作就+1
		waitgroup.Add(1)

		//这里用nowTask先存下i的值，因为goroutine是并发执行的，如果直接用i，就会导致不一致
		//最直接的错误就是index out of range
		nowTask := i

		//每个task的相关值
		task := new(DoTaskArgs)
		task.JobName = mr.jobName
		task.Phase = phase
		task.NumOtherPhase = nios
		task.TaskNumber = nowTask

		//从master中取出一个worker
		worker := <- mr.registerChannel

		//利用goroutine并发执行
		go func(phase jobPhase,  wait *sync.WaitGroup) {

			//如果是phase过程就需要有输入文件名
			if phase == mapPhase {
				task.File = mr.files[task.TaskNumber]
			}

			//远程调用worker的方法
			var reply interface{}
			if ok := call(worker, "Worker.DoTask", task, reply); ok {
				//一个任务完成，waitgroup就减去1
				waitgroup.Done()
				//把worker重新放进channel中
				mr.registerChannel <- worker
			}else {
				//看给的测试例子
				//出现错误的原因是存在worker只能执行nRPC次的调用(nRPC>0, 当nRPC < 0 时可以一直调用, 参见RunWorker源码)
				//但是出错的worker已经接受了任务,所以我们只需要找其他的worker完成该任务即可


				for ok == false {
					anotherWorker := <- mr.registerChannel
					ok = call(anotherWorker, "Worker.DoTask", task, reply)
					//完成任务的时候就把worker又放回到registerChannel中
					if ok == true {
						mr.registerChannel <- anotherWorker
					}
				}
				waitgroup.Done()

			}
			//注意是用传的waitgroup的地址，相当c++的引用，为了保持修改对象是一致的，而不是拷贝
		}(phase, &waitgroup)
	}
	fmt.Println("I am here")
	//阻塞等待所有的任务完成
	waitgroup.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
