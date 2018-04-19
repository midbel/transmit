package main

import (
	"bytes"
	"flag"
	"log"
	"io"
	"io/ioutil"
	"net"
	"time"
	"sync"
	"syscall"

	"github.com/midbel/cli"
	"golang.org/x/sync/errgroup"
)

type clock struct {}

func (_ clock) Now() time.Time {
	var t syscall.Timeval
	syscall.Gettimeofday(&t)
	s, n := t.Unix()
	return time.Unix(s, n)
}

func (_ clock) Sleep(t time.Duration) {
	s := syscall.NsecToTimespec(t.Nanoseconds())
	syscall.Nanosleep(&s, nil)
}

type writer struct {
	inner io.Writer
	bucket *Bucket
}

func Writer(w io.Writer, b *Bucket) io.Writer {
	if b == nil {
		return w
	}
	return &writer{w, b}
}

func (w *writer) Write(bs []byte) (int, error) {
	w.bucket.Take(int64(len(bs)))
	return w.inner.Write(bs)
}

type Bucket struct {
	capacity int64

	wait chan struct{}

	mu sync.Mutex
	available int64
}

func NewBucket(n int64, e time.Duration) *Bucket {
	b := &Bucket{capacity: n, available: n, wait: make(chan struct{})}
	go b.refill(e)
	return b
}

func (b *Bucket) Take(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		if d := b.available-n; b.available > 0 && d >= n {
			b.available = d
			break
		}
		<-b.wait
	}
}

func (b *Bucket) refill(e time.Duration) {
	c := float64(b.capacity*int64(e/time.Millisecond))/1000
	c *= 1.01

	ns := e.Nanoseconds()
	sleep := func() {
		i := syscall.NsecToTimespec(ns)
		syscall.Nanosleep(&i, nil)
	}

	for {
		// time.Sleep(e)
		sleep()
		if b.available > b.capacity {
			continue
		}
		b.available = b.available + int64(c)
		select {
		case b.wait <- struct{}{}:
		default:
		}
	}
}

func main() {
	var rate cli.Size
	flag.Var(&rate, "r", "rate")
	parallel := flag.Int("p", 4, "parallel")
	count := flag.Int("n", 4, "count")
	size := flag.Int("s", 1024, "size")
	buffer := flag.Int("b", 1024, "size")
	listen := flag.Bool("l", false, "listen mode")
	test := flag.Bool("t", false, "test mode")
	every := flag.Duration("e", 8*time.Millisecond, "every")
	wait := flag.Duration("w", 250*time.Millisecond, "wait")
	flag.Parse()

	var err error
	switch {
	case *test:
		err = runTest(*count, *every)
	case *listen:
		err = runServer(flag.Arg(0), *size)
	default:
		if *count <= 0 {
			*count = 1
		}
		// var b *Bucket
		// if r := rate.Int(); r > 0 {
		// 	b = NewBucket(r*int64(*count), *every)
		// }
		var g errgroup.Group
		for i := 0; i < *count; i++ {
			g.Go(func() error {
				var b *Bucket
				if r := rate.Int(); r > 0 {
					b = NewBucket(r, *every)
				}
				return runClientWithRate(flag.Arg(0), *size, *buffer, *parallel, *wait, b)
			})
		}
		err = g.Wait()
	}
	if err != nil {
		log.Fatalln(err)
	}
}

func runClientWithRate(a string, z, b, p int, e time.Duration, buck *Bucket) error {
	defer log.Println("done client")
	cs := make([]net.Conn, p)
	ws := make([]io.Writer, p)

	var as []string
	for i := 0; i < len(cs); i++ {
		c, err := net.Dial("tcp", a)
		if err != nil {
			return err
		}
		defer c.Close()
		as = append(as, c.LocalAddr().String())
		cs[i], ws[i] = c, c
		if buck != nil {
			ws[i] = Writer(c, buck)
		}
	}
	log.Printf("start client (%v)", as)
	var curr uint16

	bs := make([]byte, z)
	for {
		time.Sleep(e)
		var g errgroup.Group
		buf := bytes.NewBuffer(bs)
		for buf.Len() > 0 {
			curr++
			j := int(curr)%p
			g.Go(transmit(buf.Next(b), ws[j]))
		}
		if err := g.Wait(); err != nil {
			log.Println("exiting client", err)
			return err
		}
	}
	return nil
}

func runTest(c int, e time.Duration) error {
	k := clock{}
	a := k.Now()
	for i := 0; c <= 0 || i < c; i++ {
		k.Sleep(e)
	}
	b := k.Now()
	log.Printf("%s - %s - %s", a, b, b.Sub(a))
	return nil
}

func runServer(a string, z int) error {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return err
	}
	defer s.Close()
	for {
		c, err := s.Accept()
		if err != nil {
			return err
		}
		go func(r net.Conn) {
			defer r.Close()

			var bs []byte
			if z > 0 {
				bs = make([]byte, z)
			}

			var total float64
			w := time.Now()

			defer func() {
				d := time.Since(w)
				t := total/(1024*1024)
				log.Printf("done with %s: %.2fMB (%.2fMBs)", c.RemoteAddr(), t, t/d.Seconds())
			}()
			for {
				r.SetReadDeadline(time.Now().Add(time.Second))
				c, err := io.CopyBuffer(ioutil.Discard, r, bs)
				if err, ok := err.(net.Error); ok && err.Timeout() {
					total += float64(c)
					offset := time.Since(w)
					t := total/(1024*1024)
					log.Printf("%.2f | %.2f | %.2f | %s", float64(c)/(1024*1024), t, t/offset.Seconds(), offset)
				} else {
					return
				}
			}
		}(c)
	}
}

func transmit (bs []byte, c io.Writer) func() error {
	return func() error {
		_, err := c.Write(bs)
		return err
	}
}
