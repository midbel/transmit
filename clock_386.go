package transmit

import (
	"syscall"
	"time"
)

type sysClock struct {
	delay, threshold time.Duration
}

func (s *sysClock) Now() time.Time {
	return now()
}

func (s *sysClock) Sleep(d time.Duration) {
	if d < s.threshold {
		s.delay += d
		// s.delay = s.threshold
	} else {
		s.delay = d
	}
	if s.delay < s.threshold {
		return
	}
	var sec, nsec time.Duration
	if s.delay > time.Second {
		sec = s.delay.Truncate(time.Second)
		nsec = s.delay - sec
	} else {
		nsec = s.delay
	}
	t := syscall.Timespec{
		Sec:  int32(sec.Seconds()),
		Nsec: int32(nsec.Nanoseconds()),
	}
	syscall.Nanosleep(&t, nil)
	s.delay = 0
}

func guessThreshold() time.Duration {
	t := syscall.Timespec{
		Sec:  0,
		Nsec: int32(time.Millisecond.Nanoseconds()),
	}
	b := now()
	if err := syscall.Nanosleep(&t, nil); err != nil {
		return time.Millisecond
	}
	a := now()
	return a.Sub(b).Truncate(time.Millisecond)
}
