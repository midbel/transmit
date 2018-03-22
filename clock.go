package transmit

import (
	"syscall"
	"time"
)

type Clock interface {
	Now() time.Time
	Sleep(time.Duration)
}

func SystemClock() Clock {
	s := sysClock{threshold: guessThreshold()}
	return s
}

func RealClock() Clock {
	return realClock{}
}

type realClock struct{}

func (r realClock) Now() time.Time {
	return time.Now()
}

func (r realClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

func now() time.Time {
	var t syscall.Timeval
	if err := syscall.Gettimeofday(&t); err != nil {
		return time.Now()
	}
	return time.Unix(int64(t.Sec), int64(t.Usec*1000))
}
