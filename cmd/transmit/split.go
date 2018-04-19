package main

import (
	"bytes"
	"container/heap"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/ratelimit"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
	"github.com/midbel/transmit"
	"golang.org/x/sync/errgroup"
)

type addr struct {
	Src, Dst uint16
	IP       string
}

type Block struct {
	Id      uint32
	Sum     []byte
	Payload []byte
	Port    uint16
	Addr    *net.TCPAddr
}

func (b *Block) Stream() addr {
	a := b.Addr
	if a == nil {
		a.IP, a.Port = net.ParseIP("127.0.0.1"), int(b.Port)
	}
	return addr{Src: uint16(b.Addr.Port), Dst: b.Port, IP: b.Addr.String()}
}

func (b *Block) Chunks() []*Chunk {
	return nil
}

type BlockList struct {
	next  uint32
	queue BlockQueue
}

func List() *BlockList {
	q := make(BlockQueue, 0)
	heap.Init(&q)
	return &BlockList{next: 1, queue: q}
}

func (b *BlockList) Pop() *Block {
	if b.queue.Len() == 0 {
		return nil
	}
	x := heap.Pop(&b.queue)
	if x == nil {
		return nil
	}
	k := x.(*Block)
	if k.Id == b.next {
		b.next = k.Id + 1
		return k
	}
	b.Push(k)
	return nil
}

func (b *BlockList) Push(k *Block) {
	heap.Push(&b.queue, k)
}

type BlockQueue []*Block

func (q *BlockQueue) Push(x interface{}) {
	b, ok := x.(*Block)
	if !ok {
		return
	}
	*q = append(*q, b)
}

func (q *BlockQueue) Pop() interface{} {
	if len(*q) == 0 {
		return nil
	}
	cs := *q
	c := cs[len(cs)-1]
	*q = cs[:len(cs)-1]

	return c
}

func (q BlockQueue) Len() int {
	return len(q)
}

func (q BlockQueue) Less(i, j int) bool {
	return q[i].Id <= q[j].Id
}

func (q BlockQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

type Limiter struct {
	Rate cli.Size
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
	b := ratelimit.NewBucketWithRateAndClock(r.Float(), r.Int(), k)
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
	dtstamp time.Time
	conns   []net.Conn

	mu      sync.Mutex
	current uint32
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
		c, err := net.Dial("tcp", a)
		if err != nil {
			return nil, err
		}
		wc.conns[i], wc.writers[i] = c, k.Writer(c, n)
	}
	return &wc, nil
}

func (s *splitter) Split(b *Block) error {
	count, mod := len(b.Payload)/int(s.block), len(b.Payload)%int(s.block)
	if mod != 0 {
		count++
	}
	roll := adler32.New()
	vs := make([]byte, int(s.block))

	var g errgroup.Group
	for i, r := 0, bytes.NewReader(b.Payload); r.Len() > 0; i++ {
		n, _ := r.Read(vs)
		roll.Write(vs[:n])

		w := new(bytes.Buffer)
		w.Write(b.Sum)
		w.Write(b.Addr.IP.To16())
		binary.Write(w, binary.BigEndian, uint16(b.Addr.Port))
		binary.Write(w, binary.BigEndian, b.Port)
		binary.Write(w, binary.BigEndian, b.Id)
		binary.Write(w, binary.BigEndian, uint16(i))
		binary.Write(w, binary.BigEndian, uint16(count))
		binary.Write(w, binary.BigEndian, uint16(n))
		w.Write(vs[:n])
		binary.Write(w, binary.BigEndian, adler32.Checksum(vs[:n]))
		binary.Write(w, binary.BigEndian, roll.Sum32())

		// TODO: speed up fragment writing by running each write in its own goroutine
		// if _, err := io.Copy(s.nextWriter(), w); err != nil {
		// 	return err
		// }
		c := s.nextWriter()
		g.Go(func() error {
			_, err := io.Copy(c, w)
			return err
		})
	}
	return g.Wait()
}

func (s *splitter) nextWriter() io.Writer {
	i := atomic.AddUint32(&s.current, 1)
	return s.writers[int(i)%len(s.writers)]
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

type channel struct {
	Addr  string   `toml:"address"`
	Keep  bool     `toml:"keep"`
	Count int      `toml:"count"`
	Block cli.Size `toml:"block"`
	Rate  cli.Size `toml:"rate"`
}

func (c channel) Options(buf cli.Size, syst bool) *SplitOptions {
	i := Limiter{
		Rate: c.Rate,
		Keep: c.Keep,
		Syst: syst,
	}
	return &SplitOptions{
		Proto:  "tcp",
		Limiter: i,
		Length:  buf,
		Block:   c.Block,
		Count:   c.Count,
	}
}

var (
	LimitBandwidth = 0
	LimitListener  = 0
)

func runSplit2(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	defer f.Close()

	v := struct {
		Addr     string    `toml:"address"`
		Rate     cli.Size  `toml:"rate"`
		Block    cli.Size  `toml:"block"`
		Length   cli.Size  `toml:"buffer"`
		Syst     bool      `toml:"system"`
		Count    int       `toml:"count"`
		Channels []channel `toml:"route"`
	}{}
	if err := toml.NewDecoder(f).Decode(&v); err != nil {
		return err
	}
	if LimitListener > 0 && len(v.Channels) > LimitListener {
		return fmt.Errorf("too many listening connections configured (max allowed %d)", LimitListener)
	}
	var g errgroup.Group
	for _, c := range v.Channels {
		c := c
		g.Go(func() error {
			o := c.Options(v.Length, v.Syst)
			return listenAndSplit2(c.Addr, v.Addr, *o)
		})
	}

	return g.Wait()
}

func runSplit(cmd *cli.Command, args []string) error {
	var s SplitOptions
	s.Rate, _ = cli.ParseSize("8m")
	s.Length, _ = cli.ParseSize("32K")
	s.Block, _ = cli.ParseSize("1K")

	cmd.Flag.Var(&s.Rate, "r", "rate")
	cmd.Flag.Var(&s.Length, "s", "size")
	cmd.Flag.Var(&s.Block, "b", "block")
	cmd.Flag.IntVar(&s.Count, "n", 4, "count")
	cmd.Flag.BoolVar(&s.Keep, "k", false, "keep")
	cmd.Flag.BoolVar(&s.Syst, "y", false, "system")
	cmd.Flag.StringVar(&s.Proto, "p", "tcp", "protocol")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	if LimitListener > 0 && cmd.Flag.NArg()-1 > LimitListener {
		return fmt.Errorf("too many listening connections configured (max allowed %d)", LimitListener)
	}
	ws, err := Split(cmd.Flag.Arg(0), s.Count, int(s.Block.Int()), s.Limiter)
	if err != nil {
		return err
	}
	defer ws.Close()
	var g errgroup.Group
	for i := 1; i < cmd.Flag.NArg(); i++ {
		a := cmd.Flag.Arg(i)
		g.Go(func() error {
			return listenAndSplit(a, s.Proto, int(s.Length.Int()), ws)
		})
	}
	return g.Wait()
}

func listenAndSplit(local, proto string, block int, ws *splitter) error {
	var port uint
	if proto == "tcp" || proto == "udp" {
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
	queue := make(chan *Block, 1000)
	defer close(queue)
	switch proto {
	case "tcp", "unix":
		a, err := net.Listen(proto, local)
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
				go handle(c, uint16(port), block, queue)
			}
		}()
	default:
		return fmt.Errorf("unsupported protocol %q", proto)
	}
	for bs := range queue {
		if err := ws.Split(bs); err != nil {
			return err
		}
	}
	return nil
}

func listenAndSplit2(local, remote string, s SplitOptions) error {
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
	defer close(queue)
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
	defer c.Close()

	addr := c.RemoteAddr().(*net.TCPAddr)
	for i, bs := 1, make([]byte, n); ; i++ {
		// w := time.Now()
		c, err := c.Read(bs)
		if c == 0 || err != nil {
			return
		}
		sum := md5.Sum(bs[:c])
		b := &Block{
			Id:      uint32(i),
			Sum:     sum[:],
			Port:    p,
			Addr:    addr,
			Payload: make([]byte, c),
		}
		copy(b.Payload, bs[:c])
		queue <- b
	}
}
