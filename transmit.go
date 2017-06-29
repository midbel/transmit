package transmit

import (
	"bytes"
	"bufio"
	"crypto/rand"
	"errors"
	"encoding/binary"
	"hash/adler32"
	"io"
	"net"
	"sync/atomic"

	"github.com/midbel/uuid"
)

var (
	ErrCorrupted = errors.New("packet corrupted")
	ErrUnknownId = errors.New("unknown packet id")
)

const (
	Size    = 22
	Padding = 512
)

const (
	Copy uint16 = iota
	Reject
	Accept
	Abort
	Done
)

type Route struct{
		Id   string
		Addr string
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

	return &forwarder{
		Conn: c,
		id  : id.Bytes(),
		reader: bufio.NewReaderSize(rand.Reader, 4096),
	}, nil
}

type Router struct {
	net.Listener

	routes map[string]net.Conn
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
	}
	w, ok := r.routes[string(id)]
	if !ok {
		c.Close()
		return nil, nil, ErrUnknownId
	}
	return &forwarder{Conn: c, id: id}, &subscriber{w}, nil
}

func NewRouter(a string, rs []Route) (*Router, error) {
	l, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}
	gs := make(map[string]net.Conn)
	for _, r := range rs {
		id, _ := uuid.UUID5(uuid.URL, []byte(r.Id))
		c, err := net.Dial("udp", r.Addr)
		if err != nil {
			return nil, err
		}
		gs[id.String()] = c
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
	}
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, adler32.Checksum(d[:r]))

	return copy(b, append(d[:r], sum...)), err
}

func (s *subscriber) Write(b []byte) (int, error) {
	d, sum := b[Size:len(b)-adler32.Size], b[len(b)-adler32.Size:]
	if a := adler32.Checksum(d); a != binary.BigEndian.Uint32(sum) {
		return 0, ErrCorrupted
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
	}
	s := binary.BigEndian.Uint16(b[16:18])
	if !bytes.Equal(b[:uuid.Size], f.id) {
		return 0, ErrUnknownId
	}
	return copy(d, b[:Size+s]), err
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

	_, err := io.Copy(f.Conn, buf)
	return len(b), err
}
