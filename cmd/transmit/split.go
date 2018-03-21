package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"hash"
	"hash/adler32"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/ratelimit"
	"github.com/midbel/cli"
	"github.com/midbel/transmit"
)

type Limiter struct {
	Rate cli.Size
	Fill cli.Size
	Keep bool
	Syst bool
}

func (i Limiter) Writer(w io.Writer, c int) io.Writer {
	if i.Rate == 0 {
		return w
	}
	r := i.Rate
	if !i.Keep {
		r = i.Rate.Divide(c)
	}
	var k transmit.Clock
	if i.Syst {
		k = transmit.SystemClock()
	} else {
		k = transmit.RealClock()
	}
	y := r.Int()
	if i.Fill > 0 {
		y = i.Fill.Int()
	}
	b := ratelimit.NewBucketWithRateAndClock(r.Float(), y, k)
	return ratelimit.Writer(w, b)
}

type SplitOptions struct {
	Limiter
	Length cli.Size
	Block  cli.Size
	Count  int
}

type splitter struct {
	conns []net.Conn

	mu      sync.Mutex
	current int
	writers []io.Writer

	sequence uint32
	block    uint16
	port     uint16
	roll     hash.Hash32
}

func Split(a string, n, s int, k Limiter) (io.WriteCloser, error) {
	_, p, err := net.SplitHostPort(a)
	if err != nil {
		return nil, err
	}
	wc := splitter{
		conns:   make([]net.Conn, n),
		writers: make([]io.Writer, n),
		roll:    adler32.New(),
		block:   uint16(s),
	}
	if p, err := strconv.Atoi(p); err == nil {
		wc.port = uint16(p)
	}
	for i := 0; i < n; i++ {
		c, err := net.Dial("tcp", a)
		if err != nil {
			return nil, err
		}
		wc.conns[i], wc.writers[i] = c, k.Writer(c, n)
	}
	return &wc, nil
}

func (s *splitter) Write(bs []byte) (int, error) {
	seq := atomic.AddUint32(&s.sequence, 1)
	sum := md5.Sum(bs)

	count, mod := len(bs)/int(s.block), len(bs)%int(s.block)
	if mod != 0 {
		count++
	}
	defer s.roll.Reset()

	var t int
	vs := make([]byte, int(s.block))
	for i, r := 0, bytes.NewReader(bs); r.Len() > 0; i++ {
		n, _ := r.Read(vs)
		t += n

		s.roll.Write(vs[:n])

		w := new(bytes.Buffer)
		w.Write(sum[:])
		binary.Write(w, binary.BigEndian, s.port)
		binary.Write(w, binary.BigEndian, seq)
		binary.Write(w, binary.BigEndian, uint16(i))
		binary.Write(w, binary.BigEndian, uint16(count))
		binary.Write(w, binary.BigEndian, uint16(n))
		w.Write(vs[:n])
		binary.Write(w, binary.BigEndian, adler32.Checksum(vs[:n]))
		binary.Write(w, binary.BigEndian, s.roll.Sum32())

		if _, err := io.Copy(s.nextWriter(), w); err != nil {
			return t, err
		}
	}
	return t, nil
}

func (s *splitter) nextWriter() io.Writer {
	s.mu.Lock()
	defer s.mu.Unlock()

	w := s.writers[s.current]
	s.current = (s.current + 1) % len(s.writers)
	return w
}

func (s *splitter) Close() error {
	var err error
	for _, c := range s.conns {
		if e := c.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func runSplit(cmd *cli.Command, args []string) error {
	var s SplitOptions
	s.Rate, _ = cli.ParseSize("8m")
	s.Fill, _ = cli.ParseSize("4K")
	s.Length, _ = cli.ParseSize("32K")
	s.Block, _ = cli.ParseSize("1K")

	cmd.Flag.Var(&s.Rate, "r", "rate")
	cmd.Flag.Var(&s.Fill, "f", "fill")
	cmd.Flag.Var(&s.Length, "s", "size")
	cmd.Flag.Var(&s.Block, "b", "block")
	cmd.Flag.IntVar(&s.Count, "n", 4, "count")
	cmd.Flag.BoolVar(&s.Keep, "k", false, "keep")
	cmd.Flag.BoolVar(&s.Syst, "y", false, "system")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(cmd.Flag.NArg() - 1)
	for i := 1; i < cmd.Flag.NArg(); i++ {
		go func(a string) {
			defer wg.Done()
			if err := listenAndSplit(a, cmd.Flag.Arg(0), s); err != nil {
				log.Println(err)
			}
		}(cmd.Flag.Arg(i))
	}
	wg.Wait()
	return nil
}

func listenAndSplit(local, remote string, s SplitOptions) error {
	a, err := net.Listen("tcp", local)
	if err != nil {
		return err
	}
	defer a.Close()

	queue := make(chan []byte, 1000)
	go func() {
		for {
			c, err := a.Accept()
			if err != nil {
				return
			}
			go handle(c, int(s.Length.Int()), queue)
		}
	}()

	ws, err := Split(remote, s.Count, int(s.Block.Int()), s.Limiter)
	if err != nil {
		return err
	}
	for bs := range queue {
		if _, err := ws.Write(bs); err != nil {
			return err
		}
	}
	return nil
}

func handle(c net.Conn, n int, queue chan<- []byte) {
	defer c.Close()
	if c, ok := c.(*net.TCPConn); ok {
		c.SetKeepAlive(true)
	}

	w := time.Now()
	var count, sum int
	buf := new(bytes.Buffer)
	for i, bs := 1, make([]byte, n); ; i++ {
		for {
			n, err := c.Read(bs)
			if n == 0 && err != nil {
				return
			}
			buf.Write(bs[:n])
			if n < len(bs) {
				break
			}
		}
		sum, count = sum+buf.Len(), i
		vs := make([]byte, buf.Len())
		io.ReadFull(buf, vs)
		queue <- vs
		buf.Reset()
	}
	log.Printf("%d bytes read in %s (%d packets)", sum, time.Since(w), count)
}
