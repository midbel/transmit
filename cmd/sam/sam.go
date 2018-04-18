package main

import (
	"bytes"
	"flag"
	"log"
	"io"
	"io/ioutil"
	"net"
	"time"
	"syscall"

	"github.com/juju/ratelimit"
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

func main() {
	var rate cli.Size
	flag.Var(&rate, "r", "rate")
	parallel := flag.Int("p", 4, "parallel")
	count := flag.Int("n", 4, "count")
	size := flag.Int("s", 1024, "size")
	buffer := flag.Int("b", 1024, "size")
	listen := flag.Bool("l", false, "listen mode")
	test := flag.Bool("t", false, "test mode")
	every := flag.Duration("e", 250*time.Millisecond, "every")
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
		var buck *ratelimit.Bucket
		if r := rate.Float(); r > 0 {
			c := r/float64(*count)
			buck = ratelimit.NewBucketWithRateAndClock(r, int64(r+c), clock{})
		}
		var g errgroup.Group
		for i := 0; i < *count; i++ {
			g.Go(func() error {
				return runClient(flag.Arg(0), *size, *buffer, *parallel, buck)
			})
		}
		err = g.Wait()
	}
	if err != nil {
		log.Fatalln(err)
	}
}

func runClient(a string, z, b, p int, buck *ratelimit.Bucket) error {
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
			ws[i] = ratelimit.Writer(c, buck)
		}
	}
	log.Printf("start client (%v)", as)
	var curr uint16

	bs := make([]byte, z)
	for {
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
			for {
				r.SetReadDeadline(time.Now().Add(time.Second))
				c, err := io.CopyBuffer(ioutil.Discard, r, bs)
				if err == io.EOF {
					return
				}
				total += float64(c)
				offset := time.Since(w)
				log.Printf("%.2f | %.2f | %.2f | %s", float64(c)/1024.0, total/1024.0, total/offset.Seconds(), offset)
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
