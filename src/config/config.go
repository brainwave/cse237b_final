package constant

import "time"

const (
	// Number of workers
	WORKER_NR = 1
	// The total time of running the test
	TEST_TIME = 20 * time.Second
	// The size of the input task channels
	TASK_CHAN_SIZE = 10
	// Switch on/off preemption of the scheduler
	EN_PREEMPT = true
	// The interval to check the preemption of workers
	CHECK_PREEMPT_INTERVAL = 100 * time.Millisecond
)
