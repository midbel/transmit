package transmit

import (
  "time"
  "syscall"
)

type sysClock struct {
  delay time.Duration
}

func (s sysClock) Now() time.Time {
	var t syscall.Timeval
	if err := syscall.Gettimeofday(&t); err != nil {
		return time.Now()
	}
	return time.Unix(int64(t.Sec), int64(t.Usec*1000))
}

func (s sysClock) Sleep(d time.Duration) {
	if d <= s.delay {
		return
	}
  t := syscall.Timespec{
    Sec: 0,
    Nsec: d.Nanoseconds(),
  }
	syscall.Nanosleep(&t, nil)
}

type realClock struct {}

func (r realClock) Now() time.Time {
	return time.Now()
}

func (r realClock) Sleep(d time.Duration) {
	time.Sleep(d)
}
