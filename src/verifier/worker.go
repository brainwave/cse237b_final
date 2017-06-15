package worker

import (
	"constant"
	"log"
	"sync"
	"task"
	"time"
)

type TaskQueue struct {
	Queue []*task.Task
	Lock  *sync.Mutex
}

type WorkerPool struct {
	Pool []*Worker
	Lock *sync.Mutex
}

// Worker is the agent to process tasks
type Worker struct {
	WorkerID    int
	TaskChan    chan *task.Task
	WorkerChan  chan *Worker
	StopChan    chan interface{}
	CurTask     *task.Task
	PreEmptFlag bool
	Lock        sync.Mutex
}

// TaskProcessLoop processes tasks without preemption
func (w *Worker) TaskProcessLoop(wg *sync.WaitGroup) {
	log.Printf("Worker<%d>: Task processor starts\n", w.WorkerID)
loop:
	for {
		select {
		case t := <-w.TaskChan:
			// This worker receives a new task to run

			//snapshot the task state to current task
			w.CurTask = t

			//Display relevant message, depending upon if it was a new task or a resumed task
			if t.RunTime == time.Duration(0) {
				log.Printf("Worker <%d>: Recieved task, <%s>.Task<%d>. Processing...\n", w.WorkerID, t.AppID, t.TaskID)
			} else {
				log.Printf("Worker <%d>: %s.Task<%d> RESUMED\n", w.WorkerID, t.AppID, t.TaskID)
			}

			//Relevnt handling function, depending upon pre-emption enabled or disabled
			if constant.EN_PREEMPT == true {
				w.ProcessPreempt(t)
			} else {
				w.Process(t)
			}

			w.WorkerChan <- w

		case <-w.StopChan:
			// Receive signal to stop

			break loop
		}
	}
	log.Printf("Worker<%d>: Task processor ends\n", w.WorkerID)
	wg.Done()
}

// Process runs a task on a worker without preemption
func (w *Worker) Process(t *task.Task) {
	// Process the task
	time.Sleep(t.TotalRunTime)
	log.Printf("Worker <%d>: %s.Task<%d> ends\n", w.WorkerID, t.AppID, t.TaskID)

}

// Process runs a task on a worker with preemption
func (w *Worker) ProcessPreempt(t *task.Task) {
	// Process the task
	for {
		time.Sleep(constant.CHECK_PREEMPT_INTERVAL)
		t.RunTime += constant.CHECK_PREEMPT_INTERVAL

		//snapshot the task in execution
		w.CurTask = t

		if t.RunTime >= t.TotalRunTime {
			log.Printf("Worker <%d>: %s.Task<%d> ends\n", w.WorkerID, t.AppID, t.TaskID)
			break
		} else if w.PreEmptFlag == true && t.RunTime >= time.Duration(0) {
			log.Printf("%s.Task%d PAUSED, %v executed\n", w.CurTask.AppID, w.CurTask.TaskID, t.RunTime)
			w.Lock.Lock()
			w.PreEmptFlag = false
			w.Lock.Unlock()
			break
		}
	}

}

func (tq TaskQueue) Less(i, j int) bool {
	return (tq.Queue[i].Deadline.After(tq.Queue[j].Deadline))
}

func (tq TaskQueue) Swap(i, j int) {
	tq.Queue[i], tq.Queue[j] = tq.Queue[j], tq.Queue[i]
}

func (tq TaskQueue) Len() int {
	return (len(tq.Queue))
}
