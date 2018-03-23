package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
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

type Block struct {
	Id      uint16
	Sum     []byte
	Payload []byte
	Port    uint16
}

func (b *Block) Chunks() []*Chunk {
	return nil
}

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
	dtstamp time.Time
	conns   []net.Conn

	mu      sync.Mutex
	current int
	writers []io.Writer

	sequence uint32
	block    uint16
}

func Split(a string, n, s int, k Limiter) (*splitter, error) {
	wc := splitter{
		dtstamp: time.Now(),
		conns:   make([]net.Conn, n),
		writers: make([]io.Writer, n),
		block:   uint16(s),
	}
	for i := 0; i < n; i++ {
		c, err := transmit.Proxy(a, nil)
		if err != nil {
			return nil, err
		}
		wc.conns[i], wc.writers[i] = c, k.Writer(c, n)
	}
	return &wc, nil
}

func (s *splitter) Split(b *Block) error {
	// TODO: try to make it a kind of multiwriter
	seq := atomic.AddUint32(&s.sequence, 1)
	count, mod := len(b.Payload)/int(s.block), len(b.Payload)%int(s.block)
	if mod != 0 {
		count++
	}
	roll := adler32.New()
	vs := make([]byte, int(s.block))

	t := time.Now()
	for i, r := 0, bytes.NewReader(b.Payload); r.Len() > 0; i++ {
		n, _ := r.Read(vs)
		roll.Write(vs[:n])

		w := new(bytes.Buffer)
		w.Write(b.Sum)
		binary.Write(w, binary.BigEndian, b.Port)
		binary.Write(w, binary.BigEndian, seq)
		binary.Write(w, binary.BigEndian, uint16(i))
		binary.Write(w, binary.BigEndian, uint16(count))
		binary.Write(w, binary.BigEndian, uint16(n))
		w.Write(vs[:n])
		binary.Write(w, binary.BigEndian, adler32.Checksum(vs[:n]))
		binary.Write(w, binary.BigEndian, roll.Sum32())

		// TODO: speed up fragment writing by running each write in its own goroutine
		// TODO: maybe limited by a sema (chan struct{})
		// TODO: Write should exit as soon as any goroutine meet first an error
		// go func(w io.Writer, r io.Reader) {
		// 	if _, err := io.Copy(w, r); err != nil {
		//
		// 	}
		// }(s.nextWriter(), w)

		if _, err := io.Copy(s.nextWriter(), w); err != nil {
			return err
		}
	}
	log.Printf("%6d | %6d | %9d | %x | %16s | %16s", seq, count, len(b.Payload), b.Sum, time.Since(t), time.Since(s.dtstamp))
	return nil
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
	_, p, err := net.SplitHostPort(local)
	if err != nil {
		return err
	}
	port, err := strconv.ParseUint(p, 10, 16)
	if err != nil {
		return err
	}
	a, err := net.Listen("tcp", local)
	if err != nil {
		return err
	}
	defer a.Close()

	queue := make(chan *Block, 1000)
	go func() {
		for {
			c, err := a.Accept()
			if err != nil {
				return
			}
			go handle(c, uint16(port), int(s.Length.Int()), queue)
		}
	}()

	ws, err := Split(remote, s.Count, int(s.Block.Int()), s.Limiter)
	if err != nil {
		return err
	}
	for bs := range queue {
		if err := ws.Split(bs); err != nil {
			return err
		}
	}
	return nil
}

func handle(c net.Conn, p uint16, n int, queue chan<- *Block) {
	// TODO: try to create the chunk here instead of in the splitter
	defer c.Close()
	if c, ok := c.(*net.TCPConn); ok {
		c.SetKeepAlive(true)
	}

	sum, w := md5.New(), time.Now()
	var count, total int

	for i, buf := 0, new(bytes.Buffer); ; i++ {
		bs := make([]byte, n)
		w := io.MultiWriter(buf, sum)

		for j := 1; ; j++ {
			r, err := c.Read(bs)
			if r == 0 {
				return
			}
			w.Write(bs[:r])
			if r < n || err != nil {
				break
			}
		}
		total, count = total+buf.Len(), i+1
		if buf.Len() > 0 {
			b := &Block{
				Id:      uint16(i),
				Sum:     sum.Sum(nil),
				Port:    p,
				Payload: make([]byte, buf.Len()),
			}
			if _, err := io.ReadFull(buf, b.Payload); err == nil {
				queue <- b
			}
		}
		sum.Reset()
		buf.Reset()
	}
	log.Printf("%d bytes read in %s (%d packets)", total, time.Since(w), count)
}
