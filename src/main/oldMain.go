package main

import (
	c "constant"
	"fmt"
	"log"
	"scheduler"
	"task"
	"time"
	"worker"
)

func main() {
	apps := []*task.App{}

	// Task specifications
	taskSpecs := []task.TaskSpec{

		//Modified task specs to trigger pre-emption
		task.TaskSpec{
			Period:           6 * time.Second,
			TotalRunTimeMean: 4 * time.Second,
			TotalRunTimeStd:  0,
			RelativeDeadline: 50 * time.Second,
		},

		task.TaskSpec{
			Period:           8 * time.Second,
			TotalRunTimeMean: 2 * time.Second,
			TotalRunTimeStd:  0 * time.Millisecond,
			RelativeDeadline: 8 * time.Second,
		},
	}

	// Create all applications

	//Creating two new channels, one to communicate with workers and one to communicate with tasks.
	TaskChan := make(chan *task.Task)
	WorkerChan := make(chan *worker.Worker)

	//Send all created workers to the worker channel, indicating they are free at beginning of program execution
	for i, taskSpec := range taskSpecs {
		apps = append(apps, task.NewApp(fmt.Sprintf("app%d", i), taskSpec, TaskChan))
	}

	// Create and initialize the scheduler
	sched := scheduler.NewScheduler(TaskChan, WorkerChan)

	if sched == nil {
		log.Fatalf("Failed to create scheduler\n")
	}
	// To be implemented, initialization process

	// Start the scheduler
	sched.Start()

	// Start all applications
	for _, app := range apps {
		app.Start()
	}

	time.Sleep(c.TEST_TIME)

	// Stop all applications
	for _, app := range apps {
		app.Stop()
	}

	// Stop the scheduler
	sched.Stop()
}
