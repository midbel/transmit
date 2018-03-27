package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"io"
	"log"
	"net"
	"os"
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
	Addr    *net.TCPAddr
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
	Proto  string
}

type splitter struct {
	logger  *log.Logger
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
		logger:  log.New(os.Stderr, "[send] ", log.Ltime),
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
		w.Write(b.Addr.IP.To16())
		binary.Write(w, binary.BigEndian, uint16(b.Addr.Port))
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
	s.logger.Printf("%6d | %6d | %9d | %x | %16s | %16s", seq, count, len(b.Payload), b.Sum, time.Since(t), time.Since(s.dtstamp))
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
	cmd.Flag.StringVar(&s.Proto, "p", "tcp", "protocol")
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
	var port uint
	if s.Proto == "tcp" || s.Proto == "udp" {
		_, p, err := net.SplitHostPort(local)
		if err != nil {
			return err
		}
		if p, err := strconv.ParseUint(p, 10, 16); err != nil {
			return err
		} else {
			port = uint(p)
		}
	} else {
		defer os.Remove(local)
	}
	ws, err := Split(remote, s.Count, int(s.Block.Int()), s.Limiter)
	if err != nil {
		return err
	}
	defer ws.Close()

	queue := make(chan *Block, 1000)
	switch s.Proto {
	case "tcp", "unix":
		a, err := net.Listen(s.Proto, local)
		if err != nil {
			return err
		}
		defer a.Close()

		go func() {
			for {
				c, err := a.Accept()
				if err != nil {
					return
				}
				go handle(c, uint16(port), int(s.Length.Int()), queue)
			}
		}()
	case "udp":
		// TODO: write a dedicated handle function to deal with UDP connection
		a, err := net.ResolveUDPAddr(s.Proto, local)
		if err != nil {
			return err
		}
		c, err := net.ListenUDP(s.Proto, a)
		if err != nil {
			return err
		}
		defer c.Close()

		queue := make(chan *Block, 1000)
		go handle(c, uint16(port), int(s.Length.Int()), queue)
	default:
		return fmt.Errorf("unsupported protocol %q", s.Proto)
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
	// TODO: put handle in its own type with a logger that can log in ioutil.Discard or syslog
	// NOTE: Block type could have a Chunks() function that return a slice of Chunk
	defer c.Close()

	addr := c.RemoteAddr().(*net.TCPAddr)
	logger := log.New(os.Stderr, "[recv] ", log.Ltime)

	sum, now := md5.New(), time.Now()
	r := io.TeeReader(c, sum)
	for i, bs := 1, make([]byte, n); ; i++ {
		w := time.Now()
		c, err := r.Read(bs)
		if c == 0 || err != nil {
			return
		}
		logger.Printf("%16s | %6d | %9d | %x | %24s | %24s", addr, i, c, sum.Sum(nil), time.Since(w), time.Since(now))

		b := &Block{
			Id:      uint16(i),
			Sum:     sum.Sum(nil),
			Port:    p,
			Addr:    addr,
			Payload: make([]byte, c),
		}
		copy(b.Payload, bs[:c])
		queue <- b
		sum.Reset()
	}
}
