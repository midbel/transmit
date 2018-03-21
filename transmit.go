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
	"log"
	"net"
	"sync"
	"sync/atomic"
)

var (
	ErrCorrupted = errors.New("corrupted")
	ErrInvalid   = errors.New("invalid payload")
)

var Logger *log.Logger

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

func init() {
	Logger = log.New(ioutil.Discard, "", log.LstdFlags|log.LUTC)
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
	bs := b.Bytes()
	if err := binary.Read(r, binary.BigEndian, &p.Sum); err != nil {
		return nil, err
	}
	if p.Sum != adler32.Checksum(bs) {
		return nil, ErrCorrupted
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
			Logger.Printf("error with connection from %s: %s", c.RemoteAddr(), err)
			c.Close()
			continue
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

type subscriber struct {
	net.Conn
	port  uint16
	count uint32
}

func (s *subscriber) Read(bs []byte) (int, error) {
	vs := make([]byte, len(bs))
	n, err := s.Conn.Read(vs)
	if err != nil {
		return n, err
	}
	vs = vs[:n]

	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, s.port)
	binary.Write(b, binary.BigEndian, atomic.AddUint32(&s.count, 1))
	binary.Write(b, binary.BigEndian, uint32(n))
	b.Write(vs)
	binary.Write(b, binary.BigEndian, adler32.Checksum(b.Bytes()))

	return io.ReadFull(b, bs[:b.Len()])
}
