package transmit

import (
  "time"
  "syscall"
)

type sysClock struct {}

func (s sysClock) Now() time.Time {
	var t syscall.Timeval
	if err := syscall.Gettimeofday(&t); err != nil {
		return time.Now()
	}
	return time.Unix(int64(t.Sec), int64(t.Usec*1000))
}

func (s sysClock) Sleep(d time.Duration) {
	if d <= time.Millisecond * 2 {
		return
	}
  t := syscall.Timespec{
    Sec: 0,
    Nsec: int32(d.Nanoseconds()),
  }
	syscall.Nanosleep(&t, nil)
}
