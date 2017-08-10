package transmit

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"hash/adler32"
	"io"
	"io/ioutil"
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
	Size    = 26
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
		Conn:    c,
		id:      id.Bytes(),
		padding: Padding,
		reader:  bufio.NewReaderSize(rand.Reader, 4096),
	}
	if _, err := f.Write(f.id); err != nil {
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
	id := make([]byte, Size+uuid.Size)
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
	io.CopyN(ioutil.Discard, c, Padding)
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
		go log.Printf("%d bytes read from %s (%x)", r, s.LocalAddr(), md5.Sum(d))
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
	if err == nil {
		go log.Printf("%d bytes written to %s (%x)", len(d), s.RemoteAddr(), md5.Sum(d))
	}
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
	d, err := readFrom(f.Conn, f.id)
	go log.Printf("%d bytes received from %s", len(d), f.RemoteAddr())
	return copy(b, d), err
}

func (f *forwarder) Write(b []byte) (int, error) {
	s := atomic.AddUint32(&f.sequence, 1)
	err := writeTo(f.Conn, s, f.id, b)
	go log.Printf("%d bytes sent to %s", len(b), f.RemoteAddr())
	return len(b), err
}

type block struct {
	Id       [uuid.Size]byte
	Length   uint16
	Sequence uint32
	N        uint8
	C        uint8
	R        uint16
}

func readFrom(r io.Reader, i []byte) ([]byte, error) {
	w := new(bytes.Buffer)

	for {
		v := new(block)
		if err := binary.Read(r, binary.BigEndian, v); err != nil {
			return nil, err
		}
		if _, err := io.CopyN(w, r, int64(v.R)); err != nil {
			return nil, err
		}
		if v.R < Padding {
			io.CopyN(ioutil.Discard, r, Padding)
		}
		if !bytes.Equal(v.Id[:], i) {
			return nil, ErrUnknownId
		}
		if v.N == v.C {
			break
		}
	}
	return w.Bytes(), nil
}

func writeTo(w io.Writer, s uint32, id, b []byte) error {
	const size = 1024
	r := bytes.NewReader(b)

	n := r.Size() / size
	/*if m := r.Size() % size; n > 0 && m != 0 {
		n += 1
	}*/
	var j uint8
	for r.Len() > 0 {
		buf := new(bytes.Buffer)

		buf.Write(id)
		binary.Write(buf, binary.BigEndian, s)
		binary.Write(buf, binary.BigEndian, uint16(len(b)))
		binary.Write(buf, binary.BigEndian, uint8(n))
		binary.Write(buf, binary.BigEndian, uint8(j))

		count := size
		if r.Len() < size {
			count = r.Len()
		}
		binary.Write(buf, binary.BigEndian, uint16(count))

		io.CopyN(buf, r, int64(count))
		if count < Padding {
			io.CopyN(buf, rand.Reader, Padding)
		}
		if _, err := io.Copy(w, buf); err != nil {
			return err
		}
		j++
	}
	return nil
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
