package transmit

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"hash/adler32"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrCorrupted = errors.New("corrupted")
	ErrInvalid   = errors.New("invalid payload")
)

type Transmitter interface {
	Transmit(*Packet) error
}

type Packet struct {
	Port     uint16
	Sequence uint32
	Length   uint32
	Payload  []byte
	Sum      uint32
}

func EncodePacket(p *Packet) ([]byte, error) {
	w := new(bytes.Buffer)

	binary.Write(w, binary.BigEndian, p.Port)
	binary.Write(w, binary.BigEndian, p.Sequence)
	binary.Write(w, binary.BigEndian, p.Length)
	w.Write(p.Payload)
	binary.Write(w, binary.BigEndian, adler32.Checksum(w.Bytes()))

	return w.Bytes(), nil
}

func DecodePacket(r io.Reader) (*Packet, error) {
	p := new(Packet)
	b := new(bytes.Buffer)

	r = io.TeeReader(r, b)
	if err := binary.Read(r, binary.BigEndian, &p.Port); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Sequence); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Length); err != nil {
		return nil, err
	}

	p.Payload = make([]byte, int(p.Length))
	if _, err := io.ReadFull(r, p.Payload); err != nil {
		return nil, ErrInvalid
	}
	if err := binary.Read(r, binary.BigEndian, &p.Sum); err != nil {
		return nil, err
	}

	return p, nil
}

func Listen(a string, c *tls.Config, mux *PortMux) error {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return err
	}
	if c != nil {
		s = tls.NewListener(s, c)
	}
	for {
		c, err := s.Accept()
		if err != nil {
			return err
		}
		if c, ok := c.(*net.TCPConn); ok {
			c.SetKeepAlive(true)
		}
		go mux.Handle(c)
	}
	return nil
}

type PortMux struct {
	mu      sync.RWMutex
	writers map[uint16][]io.Writer
}

func NewPortMux() *PortMux {
	return &PortMux{writers: make(map[uint16][]io.Writer)}
}

func (p *PortMux) Handle(c net.Conn) {
	defer c.Close()

	r := bufio.NewReader(c)
	for {
		k, err := DecodePacket(r)
		if err != nil {
			return
		}
		p.Transmit(k)
	}
}

func (p *PortMux) Register(d uint16, w io.Writer) {
	p.mu.Lock()
	p.writers[d] = append(p.writers[d], w)
	p.mu.Unlock()
}

func (p *PortMux) Transmit(k *Packet) error {
	p.mu.RLock()
	ws, ok := p.writers[k.Port]
	p.mu.RUnlock()

	if !ok {
		return nil
	}

	for _, w := range ws {
		switch w.(type) {
		case *net.TCPConn, *proxy:
			bs, _ := EncodePacket(k)
			w.Write(bs)
		case *net.UDPConn:
			w.Write(k.Payload)
		}
	}
	return nil
}

func Proxy(a string, c *tls.Config) (net.Conn, error) {
	s, err := net.Dial("tcp", a)
	if err != nil {
		return nil, err
	}
	if c != nil {
		s = tls.Client(s, c)
	}
	return &proxy{Conn: s, writer: s, cert: c, addr: a}, nil
}

func Subscribe(d, i string, p uint16) (net.Conn, error) {
	a, err := net.ResolveUDPAddr("udp", d)
	if err != nil {
		return nil, err
	}
	var ifi *net.Interface
	if i, err := net.InterfaceByName(i); err == nil {
		ifi = i
	}
	c, err := net.ListenMulticastUDP("udp", ifi, a)
	if err != nil {
		return nil, err
	}
	return &subscriber{Conn: c, port: p}, nil
}

type proxy struct {
	net.Conn

	addr string
	cert *tls.Config

	mu     sync.Mutex
	writer io.Writer
}

func (p *proxy) Write(bs []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, err := p.writer.Write(bs)
	if e, ok := err.(net.Error); ok && !e.Temporary() {
		p.Conn.Close()
		p.writer, p.Conn = ioutil.Discard, nil

		go p.redial()
		err = nil
	}
	return len(bs), nil
}

func (p *proxy) redial() {
	var err error
	for i := 0; ; i++ {
		p.Conn, err = net.DialTimeout("tcp", p.addr, time.Second*5)
		if err == nil {
			break
		}
		s := (i % 5) + 1
		time.Sleep(time.Second * time.Duration(s))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writer = p.Conn
}

type subscriber struct {
	net.Conn
	port  uint16
	count uint32
}

func (s *subscriber) Read(bs []byte) (int, error) {
	vs := make([]byte, len(bs))
	n, _ := s.Conn.Read(vs)

	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, s.port)
	binary.Write(b, binary.BigEndian, atomic.AddUint32(&s.count, 1))
	binary.Write(b, binary.BigEndian, uint32(n))
	b.Write(vs[:n])
	binary.Write(b, binary.BigEndian, adler32.Checksum(vs[:n]))

	return io.ReadFull(b, bs[:b.Len()])
}
