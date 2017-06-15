package task

import (
	"log"
	"math/rand"
	"time"
)

// TaskSpec is the specification of a task
type TaskSpec struct {
	Period           time.Duration
	TotalRunTimeMean time.Duration
	TotalRunTimeStd  time.Duration
	RelativeDeadline time.Duration // deadline related to the start time
}

// Task is the task instance
type Task struct {
	AppID        string
	TaskID       int
	StartTime    time.Time
	RunTime      time.Duration
	TotalRunTime time.Duration
	Deadline     time.Time // absolute deadline timestamp
}

// App is an application that generate tasks periodically
type App struct {
	AppID     string
	TaskID    int
	Spec      TaskSpec
	TaskTimer *time.Ticker
	StopChan  chan interface{}
	TaskChan  chan *Task // the channel through which the application submits new task to the scheduler
}

// NewApp creates a new application
func NewApp(appID string, spec TaskSpec, tsk chan *Task) *App {
	return &App{
		AppID:     appID,
		TaskID:    0,
		Spec:      spec,
		TaskTimer: nil,
		StopChan:  make(chan interface{}),
		TaskChan:  tsk,
	}
}

// NewTask creates a new task
func (a *App) NewTask(startTime time.Time) (task *Task) {
	mean, std := a.Spec.TotalRunTimeMean.Nanoseconds(), a.Spec.TotalRunTimeStd.Nanoseconds()
	task = &Task{
		AppID:        a.AppID,
		TaskID:       a.TaskID,
		StartTime:    startTime,
		RunTime:      time.Duration(0),
		TotalRunTime: time.Duration(int(rand.NormFloat64()*float64(std) + float64(mean))),
		Deadline:     startTime.Add(a.Spec.RelativeDeadline),
	}
	a.TaskID++
	return
}

// TaskGenerateLoop runs inside a goroutine to generate tasks periodically
func (a *App) TaskGenerateLoop() {
	if a.StopChan == nil {
		log.Fatalf("%s: Stop channel not assigned\n", a.AppID)
	}
	log.Printf("%s: Task generator starts\n", a.AppID)
	a.TaskTimer = time.NewTicker(a.Spec.Period)
loop:
	for {
		select {
		case <-a.TaskTimer.C:
			// Timer timeouts, create a new task
			startTime := time.Now()
			t := a.NewTask(startTime)
			log.Printf("%s>: Create task<%d>, rt %v, ddl %v, \n", a.AppID, t.TaskID, t.TotalRunTime, t.Deadline)
			a.TaskChan <- t

		case <-a.StopChan:
			// receive signal to stop creating tasks
			log.Printf("%s: Stopped task creation\n", a.AppID)
			break loop
		}
	}
	a.TaskTimer.Stop()
	log.Printf("%s: Task generator ends\n", a.AppID)
}

// Start starts this application
func (a *App) Start() {
	// Create a goroutine to create tasks for the app
	go a.TaskGenerateLoop()
}

// Stop stops this application
func (a *App) Stop() {
	a.StopChan <- 0
}
