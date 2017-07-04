package transmit

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"hash/adler32"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/midbel/uuid"
)

var ErrClosed = errors.New("closed")

var (
	ErrCorrupted = errors.New("packet corrupted")
	ErrUnknownId = errors.New("unknown packet id")
)

const (
	Size    = 22
	Padding = 512
)

const (
	Bind uint16 = iota
	Accept
	Reject
	Copy
	Abort
	Done
)

type Route struct {
	Id   string `json:"id"`
	Addr string `json:"addr"`
	Eth  string `json:"ifi"`
}

func Subscribe(a, n string) (net.Conn, error) {
	addr, err := net.ResolveUDPAddr("udp", a)
	if err != nil {
		return nil, err
	}
	i, err := net.InterfaceByName(n)
	if err != nil && len(n) > 0 {
		return nil, err
	}
	c, err := net.ListenMulticastUDP("udp", i, addr)
	if err != nil {
		return nil, err
	}
	return &subscriber{c}, nil
}

func Dispatch(a string) (net.Conn, error) {
	c, err := net.Dial("udp", a)
	if err != nil {
		return nil, err
	}
	return &subscriber{c}, nil
}

func Forward(a, s string) (net.Conn, error) {
	c, err := net.Dial("tcp", a)
	if err != nil {
		return nil, err
	}
	id, _ := uuid.UUID5(uuid.URL, []byte(s))

	f := &forwarder{
		Conn:   c,
		id:     id.Bytes(),
		padding: Padding,
		reader: bufio.NewReaderSize(rand.Reader, 4096),
	}
	if _, err := f.Write([]byte{}); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

type Router struct {
	net.Listener

	routes map[string]*pool
}

func (r *Router) Accept() (net.Conn, net.Conn, error) {
	c, err := r.Listener.Accept()
	if err != nil {
		return nil, nil, err
	}
	id := make([]byte, Size)
	if _, err := io.ReadFull(c, id); err != nil {
		c.Close()
		return nil, nil, err
	} else {
		id = id[:uuid.Size]
	}
	p, ok := r.routes[string(id)]
	if !ok {
		c.Close()
		return nil, nil, ErrUnknownId
	}
	w, err := p.Acquire()
	if err != nil {
		return nil, nil, err
	}
	return &forwarder{Conn: c, id: id}, w, nil
}

func (r *Router) Close() error {
	err := r.Listener.Close()
	for _, p := range r.routes {
		if e := p.Close(); err == nil && e != nil {
			err = e
		}
	}
	return err
}

func NewRouter(a string, rs []Route) (*Router, error) {
	l, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}
	gs := make(map[string]*pool)
	for _, r := range rs {
		id, _ := uuid.UUID5(uuid.URL, []byte(r.Id))
		p := &pool{
			c: make(chan net.Conn, 5),
			a: r.Addr,
		}
		gs[string(id.Bytes())] = p
	}
	return &Router{Listener: l, routes: gs}, nil
}

type subscriber struct {
	net.Conn
}

func (s *subscriber) Read(b []byte) (int, error) {
	d := make([]byte, len(b))
	r, err := s.Conn.Read(d)
	if err != nil && r == 0 {
		return r, err
	} else {
		d = d[:r]
	}
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, adler32.Checksum(d))
	d = append(d, sum...)

	return copy(b, d), err
}

func (s *subscriber) Write(b []byte) (int, error) {
	d, sum := b[:len(b)-adler32.Size], b[len(b)-adler32.Size:]
	if a := adler32.Checksum(d); a != binary.BigEndian.Uint32(sum) {
		return len(b), ErrCorrupted
	}
	_, err := s.Conn.Write(d)
	return len(b), err
}

type forwarder struct {
	net.Conn

	id []byte

	sequence uint32
	padding  uint16

	reader io.Reader
}

func (f *forwarder) Read(b []byte) (int, error) {
	d := make([]byte, len(b))
	r, err := f.Conn.Read(d)
	if err != nil && r == 0 {
		return r, err
	} else {
		d = d[:r]
		go log.Printf("%d bytes read from %s", r, f.LocalAddr())
	}
	if !bytes.Equal(d[:uuid.Size], f.id) {
		return r, ErrUnknownId
	}
	s := binary.BigEndian.Uint16(d[16:18])
	return copy(b, d[Size:Size+s]), err
}

func (f *forwarder) Write(b []byte) (int, error) {
	buf := new(bytes.Buffer)

	buf.Write(f.id)
	binary.Write(buf, binary.BigEndian, uint16(len(b)))
	binary.Write(buf, binary.BigEndian, atomic.AddUint32(&f.sequence, 1))
	buf.Write(b)

	if buf.Len() < int(f.padding) {
		io.CopyN(buf, f.reader, int64(f.padding))
	}

	w, err := io.Copy(f.Conn, buf)
	if err == nil {
		go log.Printf("%d bytes written to %s", w, f.RemoteAddr())
	}
	return len(b), err
}

type pool struct {
	a string
	c chan net.Conn

	closed bool
	mu     sync.Mutex
}

func (p *pool) Acquire() (net.Conn, error) {
	select {
	case c, ok := <-p.c:
		if !ok {
			return nil, ErrClosed
		}
		return c, nil
	default:
		return Dispatch(p.a)
	}
}

func (p *pool) Release(c net.Conn) error {
	if p.closed {
		return ErrClosed
	}
	select {
	case p.c <- c:
		return nil
	default:
		return c.Close()
	}
}

func (p *pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	var err error
	for c := range p.c {
		if e := c.Close(); err == nil && e != nil {
			err = e
		}
	}
	close(p.c)
	return err
}
