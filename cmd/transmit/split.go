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

	"github.com/juju/ratelimit"
	"github.com/midbel/cli"
)

type SplitOptions struct {
	Rate   cli.Size
	Length cli.Size
	Block  cli.Size
	Syst   bool
	Count  int
}

type incoming struct {
	net.Conn
	buffer []byte
}

func (i *incoming) Read(bs []byte) (int, error) {
	n, err := i.Conn.Read(i.buffer)
	return copy(bs, i.buffer[:n]), err
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

func Split(a string, n, s int, r float64) (io.WriteCloser, error) {
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
	if p, err := strconv.Atoi(p); err != nil {
		wc.port = uint16(p)
	}
	for i := 0; i < n; i++ {
		c, err := net.Dial("tcp", a)
		if err != nil {
			return nil, err
		}
		var w io.Writer = c
		if r > 0 {
			b := ratelimit.NewBucketWithRateAndClock(r, int64(s), nil)
			w = ratelimit.Writer(w, b)
		}
		wc.conns[i], wc.writers[i] = c, w
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
	cmd.Flag.Var(&s.Rate, "r", "rate")
	cmd.Flag.Var(&s.Length, "s", "size")
	cmd.Flag.Var(&s.Block, "b", "block")
	cmd.Flag.IntVar(&s.Count, "n", 4, "count")
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

	ws, err := Split(remote, s.Count, int(s.Block.Int()), s.Rate.Float())
	if err != nil {
		return err
	}
	defer ws.Close()
	for {
		c, err := a.Accept()
		if err != nil {
			return err
		}
		defer c.Close()
		r := &incoming{
			Conn:   c,
			buffer: make([]byte, s.Length.Int()),
		}
		go io.Copy(ws, r)
	}
	return nil
}
