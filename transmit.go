package transmit

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"hash/adler32"
	"io"
	"net"
	"sync/atomic"
)

var ErrCorrupted = errors.New("corrupted")

type Packet struct {
	Port     uint16
	Sequence uint32
	Length   uint32
	Payload  []byte
	Sum      uint32
}

func DecodePacket(r io.Reader) (*Packet, error) {
	p := new(Packet)
	w := new(bytes.Buffer)

	r = io.TeeReader(r, w)
	binary.Read(r, binary.BigEndian, &p.Port)
	binary.Read(r, binary.BigEndian, &p.Sequence)
	binary.Read(r, binary.BigEndian, &p.Length)

	p.Payload = make([]byte, int(p.Length))
	if _, err := io.ReadFull(r, p.Payload); err != nil {
		return nil, err
	}
	sum := adler32.Checksum(w.Bytes())
	binary.Read(r, binary.BigEndian, &p.Sum)
	if sum != p.Sum {
		return nil, ErrCorrupted
	}
	return p, nil
}

func EncodePacket(p *Packet) []byte {
	w := new(bytes.Buffer)

	binary.Write(w, binary.BigEndian, p.Port)
	binary.Write(w, binary.BigEndian, p.Sequence)
	binary.Write(w, binary.BigEndian, uint32(len(p.Payload)))
	w.Write(p.Payload)

	binary.Write(w, binary.BigEndian, adler32.Checksum(w.Bytes()))

	return w.Bytes()
}

func Subscribe(g string) (net.Conn, error) {
	addr, err := net.ResolveUDPAddr("udp", g)
	if err != nil {
		return nil, err
	}
	return net.ListenMulticastUDP("udp", nil, addr)
}

type forwarder struct {
	net.Conn

	port uint16
	curr uint32
}

func Forward(a string, p int, c *tls.Config) (net.Conn, error) {
	var conn net.Conn

	addr, err := net.ResolveTCPAddr("tcp", a)
	if err != nil {
		return nil, err
	}
	if conn, err = net.DialTCP("tcp", nil, addr); err != nil {
		return nil, err
	}
	if c != nil {
		conn = tls.Client(conn, c)
	}
	if p <= 0 {
		p = addr.Port
	}
	return &forwarder{Conn: conn, port: uint16(p)}, nil
}

func (f *forwarder) Write(bs []byte) (int, error) {
	defer atomic.AddUint32(&f.curr, 1)

	p := &Packet{
		Port:     f.port,
		Sequence: f.curr,
		Payload:  bs,
	}
	if n, err := f.Conn.Write(EncodePacket(p)); err != nil {
		return n, err
	}
	return len(bs), nil
}
