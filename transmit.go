package transmit

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"hash/adler32"
	"io"
	"net"
	"sync/atomic"
)

type Packet struct {
	Port     uint16
	Sequence uint32
	Length   uint32
	Payload  []byte
	Sum      uint32
}

func DecodePacket(r io.Reader) (*Packet, error) {
	p := new(Packet)
	binary.Read(r, binary.BigEndian, &p.Port)
	binary.Read(r, binary.BigEndian, &p.Sequence)
	binary.Read(r, binary.BigEndian, &p.Length)

	p.Payload = make([]byte, int(p.Length))
	if _, err := io.ReadFull(r, p.Payload); err != nil {
		return nil, err
	}
	return p, nil
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

	w := new(bytes.Buffer)
	binary.Write(w, binary.BigEndian, f.port)
	binary.Write(w, binary.BigEndian, f.curr)
	binary.Write(w, binary.BigEndian, uint32(len(bs)))
	w.Write(bs)
	binary.Write(w, binary.BigEndian, adler32.Checksum(w.Bytes()))

	if n, err := io.Copy(f.Conn, w); err != nil {
		return int(n), err
	}
	return len(bs), nil
}
