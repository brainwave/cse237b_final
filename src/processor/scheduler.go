package scheduler

import (
	"constant"
	"log"
	"sort"
	"sync"
	"task"
	"worker"
)

// Scheduler dispatches tasks to workers
type Scheduler struct {
	TaskChan      chan *task.Task
	WorkerChan    chan *worker.Worker
	StopChan      chan interface{}
	FreeWorkerBuf *worker.WorkerPool
	AllWorkerBuf  *worker.WorkerPool
	TaskBuf       *worker.TaskQueue
	wg            sync.WaitGroup
}

//NewScheduler assigns global channels created in main.go
func NewScheduler(t chan *task.Task, w chan *worker.Worker) *Scheduler {
	log.Printf("Creating new scheduler\n")

	//Create a dummy scheduler object
	sched := Scheduler{
		TaskChan:      t,
		WorkerChan:    w,
		StopChan:      make(chan interface{}),
		FreeWorkerBuf: &worker.WorkerPool{},
		AllWorkerBuf:  &worker.WorkerPool{},
		TaskBuf:       &worker.TaskQueue{},
	}

	log.Println("Scheduler started")
	return &sched
}

// ScheduleLoop runs the scheduling algorithm inside a goroutine
func (s *Scheduler) ScheduleLoop() {
	log.Printf("Scheduler: Scheduling loop starts\n")
loop:
	for {
		select {
		case newTask := <-s.TaskChan:

			// Receive a new task and insert into a queue of all Tasks
			s.TaskBuf.Queue = append(s.TaskBuf.Queue, newTask)
			sort.Sort(s.TaskBuf)

			//Find out how many tasks we have to execute, and how many workers we have available to us
			TaskLen := len(s.TaskBuf.Queue) - 1
			WrkrLen := len(s.FreeWorkerBuf.Pool) - 1

			//Sanity check on the task priority queue
			for i, qIter := range s.TaskBuf.Queue {
				if i != 0 && s.TaskBuf.Queue[i-1].Deadline.Before(qIter.Deadline) {
					log.Fatalf("Scheduling error")
				}
			}

			log.Printf("Scheduler: New Task: %s.Task%d. TaskQueue Len: %d\n", newTask.AppID, newTask.TaskID, TaskLen+1)

			//If there are 1 or more tasks, and 1 or more free workers
			if TaskLen >= 0 {
				if WrkrLen >= 0 {

					//Assign the task with earliest deadline to a worker
					//Remove the assigned task from task queue, and assigned worker from free worker queue
					log.Printf("Scheduler: %s.Task%d -> Worker %d)\n", s.TaskBuf.Queue[TaskLen].AppID, s.TaskBuf.Queue[TaskLen].TaskID, s.FreeWorkerBuf.Pool[WrkrLen].WorkerID)

					s.FreeWorkerBuf.Pool[WrkrLen].TaskChan <- s.TaskBuf.Queue[TaskLen]
					s.FreeWorkerBuf.Pool = s.FreeWorkerBuf.Pool[:WrkrLen]
					s.TaskBuf.Queue = s.TaskBuf.Queue[:TaskLen]

				} else if constant.EN_PREEMPT == true {

					//Allow use of preemption depending upon flag
					for _, wrkr := range s.AllWorkerBuf.Pool {

						if wrkr.CurTask.Deadline.After(newTask.Deadline) {
							log.Printf("Worker %d executing %s.Task%d, preempted for %s.Task%d\n", wrkr.WorkerID, wrkr.CurTask.AppID, wrkr.CurTask.TaskID, newTask.AppID, newTask.TaskID)

							s.TaskBuf.Queue = append(s.TaskBuf.Queue, wrkr.CurTask)

							wrkr.Lock.Lock()
							wrkr.PreEmptFlag = true
							wrkr.Lock.Unlock()

							sort.Sort(s.TaskBuf)
							break
						}
					}
				}

			}

		case w := <-s.WorkerChan:

			//This handles the case when a worker becomes free

			// Assign the worker a new task, and remove the task from queue
			s.FreeWorkerBuf.Pool = append(s.FreeWorkerBuf.Pool, w)

			TaskLen := len(s.TaskBuf.Queue) - 1
			WrkrLen := len(s.FreeWorkerBuf.Pool) - 1

			//log.Printf("Scheduler: Worker %d free, inserted. num free workers: %d", w.WorkerID, len(s.FreeWorkerBuf.Pool))

			if TaskLen >= 0 && WrkrLen >= 0 {

				//Same as the new task case
				log.Printf("Scheduler: %s.Task%d->Worker %d\n",
					s.TaskBuf.Queue[TaskLen].AppID, s.TaskBuf.Queue[TaskLen].TaskID, w.WorkerID)

				s.FreeWorkerBuf.Pool[WrkrLen].TaskChan <- s.TaskBuf.Queue[TaskLen]
				s.FreeWorkerBuf.Pool = s.FreeWorkerBuf.Pool[:WrkrLen]
				s.TaskBuf.Queue = s.TaskBuf.Queue[:TaskLen]

				//(2) indicates that the handover was caused by worker becoming free
				//	log.Printf("Scheduler: Task Handover Complete(2), Queue Length %d\n", len(s.TaskBuf.Queue))
			}

		case <-s.StopChan:
			// Receive signal to stop scheduling
			log.Printf("Scheduler: Stop Signal\n")

			break loop

		}

	}
	log.Printf("Scheduler: Task processor ends\n")
}

// Start starts the scheduler
func (s *Scheduler) Start() {

	go s.ScheduleLoop()

	for iter := 1; iter <= constant.WORKER_NR; iter++ {
		//Create new workers as per constants file
		//Also handles initialization of new workers, and assignment to relevant queues
		wrkr := new(worker.Worker)
		wrkr.WorkerID = iter
		wrkr.TaskChan = make(chan *task.Task)
		wrkr.WorkerChan = s.WorkerChan
		wrkr.StopChan = make(chan interface{})
		//Add them to worker pool
		s.FreeWorkerBuf.Pool = append(s.FreeWorkerBuf.Pool, wrkr)
		s.AllWorkerBuf.Pool = append(s.AllWorkerBuf.Pool, wrkr)

		s.wg.Add(1)
		go wrkr.TaskProcessLoop(&s.wg)
	}

}

// Stop stops the scheduler
func (s *Scheduler) Stop() {

	for _, w := range s.AllWorkerBuf.Pool {
		w.StopChan <- 0
	}
	s.wg.Wait()
	s.StopChan <- 0

}

/* Heap implementation, deferred to later, work in progress
func (tq worker.TaskQueue) Less(i, j int) bool {
	return (tq.Queue[i].Deadline.Before(tq.Queue[j].Deadline))
}

func (tq worker.TaskQueue) Push(newTask task.Task) {
	tq.Queue = append(tq.Queue, newTask)
	log.Printf("Scheduler: Heap - Task%d, %s inserted\n", newTask.TaskID, newTask.AppID)
}

func (tq worker.TaskQueue) Pop() worker.TaskQueue {
	tqlen = len(tq.Queue) - 1
	log.Printf("Scheduler: Heap - Task%d, %s removed\n", tq.Queue[tqlen].TaskID, tq.Queue[tqlen].AppID)
	tq.Queue = tq.Queue[:tqlen]
}
*/
