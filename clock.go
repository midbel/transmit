package transmit

import (
	"time"
)

type Clock interface {
	Now() time.Time
	Sleep(time.Duration)
}

func SystemClock() Clock {
	return sysClock{time.Millisecond}
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
