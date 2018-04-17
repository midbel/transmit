package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/adler32"
	"io"
	// "log"
	"net"
	"os"
	"sort"
	"sync"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

var (
	ErrDone     = errors.New("done")
	ErrRollSum  = errors.New("rolling checksum mismatched")
	ErrChecksum = errors.New("md5 checksum mismatched")
)

type Chunk struct {
	Key
	Frag    uint16
	Count   uint16
	Length  uint16
	Payload []byte
	Roll    uint32
}

type nat struct {
	Addr string
	Port uint16
}

type Key struct {
	Id  uint32
	Dst uint16
	IP  [net.IPv6len]byte
	Src uint16
	Sum [md5.Size]byte
}

func (k Key) Route() interface{} {
	addr := &net.TCPAddr{IP: net.IP(k.IP[:]), Port: int(k.Src)}
	v := struct {
		Port uint16
		Addr string
	}{
		Port: k.Dst,
		Addr: addr.String(),
	}
	return v
}

func (k Key) Equal(s []byte) bool {
	return bytes.Equal(s, k.Sum[:])
}

type merger struct {
	inner net.Listener

	mu     sync.Mutex
	chunks map[Key][]*Chunk

	errors chan error
	blocks chan *Block
}

func Merge(a string) (*merger, error) {
	c, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}
	m := &merger{
		inner:  c,
		chunks: make(map[Key][]*Chunk),
		errors: make(chan error),
		blocks: make(chan *Block),
	}
	go m.listen()
	return m, nil
}

func (m *merger) Next() (*Block, error) {
	select {
	case b, ok := <-m.blocks:
		if !ok {
			break
		}
		return b, nil
	case e, ok := <-m.errors:
		if !ok {
			break
		}
		return nil, e
	}
	return nil, ErrDone
}

func (m *merger) build(k Key, cs []*Chunk) {
	buf := new(bytes.Buffer)
	roll, sum := adler32.New(), md5.New()
	w := io.MultiWriter(buf, sum, roll)

	sort.Slice(cs, func(i, j int) bool {
		return cs[i].Frag <= cs[j].Frag
	})
	for _, i := range cs {
		w.Write(i.Payload)
		if s := roll.Sum32(); s != i.Roll {
			m.errors <- ErrRollSum
			// return nil, ErrRollSum
			return
		}
	}
	if s := sum.Sum(nil); !k.Equal(s) {
		m.errors <- ErrChecksum
		// return nil, ErrChecksum
		return
	}
	b := &Block{
		Id:      k.Id,
		Port:    k.Dst,
		Sum:     k.Sum[:],
		Addr:    &net.TCPAddr{IP: k.IP[:], Port: int(k.Src)},
		Payload: make([]byte, buf.Len()),
	}
	if _, err := io.ReadFull(buf, b.Payload); err != nil {
		m.errors <- err
		return
	}
	m.blocks <- b
}

func (m *merger) merge(c *Chunk) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cs := append(m.chunks[c.Key], c)
	if len(cs) >= int(c.Count) {
		delete(m.chunks, c.Key)
		go m.build(c.Key, cs)
	} else {
		m.chunks[c.Key] = cs
	}
}

func (m *merger) listen() {
	for {
		c, err := m.inner.Accept()
		if err != nil {
			return
		}
		if c, ok := c.(*net.TCPConn); ok {
			c.SetKeepAlive(true)
		}
		go func(r net.Conn) {
			defer r.Close()
			for {
				c, err := decodeChunk(r)
				if err != nil {
					return
				}
				m.merge(c)
			}
		}(c)
	}
}

type forwarder struct {
	addrs  map[uint16]net.Addr
	conns  map[addr]net.Conn
	blocks map[addr]*BlockList
}

func ForwardMap(as map[string][]uint16) (*forwarder, error) {
	f := &forwarder{
		addrs:  make(map[uint16]net.Addr),
		conns:  make(map[addr]net.Conn),
		blocks: make(map[addr]*BlockList),
	}
	for a, ps := range as {
		a, err := net.ResolveTCPAddr("tcp", a)
		if err != nil {
			return nil, err
		}
		if len(ps) == 0 {
			f.addrs[uint16(a.Port)] = a
		} else {
			for _, p := range ps {
				f.addrs[p] = a
			}
		}
	}
	return f, nil
}

func Forward(as ...string) (*forwarder, error) {
	// vs := make(map[string][]uint16)
	// for _, a := range as {
	// 	vs[a] = nil
	// }
	// return ForwardMap(vs)
	f := &forwarder{
		addrs:  make(map[uint16]net.Addr),
		conns:  make(map[addr]net.Conn),
		blocks: make(map[addr]*BlockList),
	}
	for _, a := range as {
		a, err := net.ResolveTCPAddr("tcp", a)
		if err != nil {
			return nil, err
		}
		f.addrs[uint16(a.Port)] = a
	}
	return f, nil
}

func (f *forwarder) Forward(b *Block) error {
	a, ok := f.addrs[b.Port]
	if !ok {
		return fmt.Errorf("no route configued to %d", b.Port)
	}
	s := b.Stream()
	q, ok := f.blocks[s]
	if !ok {
		q = List()
		f.blocks[s] = q
	}
	q.Push(b)

	var err error
	c, ok := f.conns[s]
	if !ok {
		c, err = net.Dial(a.Network(), a.String())
		if err != nil {
			return err
		}
		f.conns[s] = c
	}
	for b := q.Pop(); b != nil; b = q.Pop() {
		_, err = c.Write(b.Payload)
		if err != nil {
			return err
		}
	}
	return nil
}

type mapping struct {
	Addr  string   `toml:"address"`
	Ports []uint16 `toml:"port"`
}

func runMerge2(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	r, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	defer r.Close()

	c := struct {
		Addr string `toml:"address"`
		Mappings []mapping `toml:"channel"`
	}{}
	if err := toml.NewDecoder(r).Decode(&c); err != nil {
		return err
	}
	as := make(map[string][]uint16)
	for _, c := range c.Mappings {
		as[c.Addr] = c.Ports
	}
	f, err := ForwardMap(as)
	if err != nil {
		return err
	}
	m, err := Merge(c.Addr)
	if err != nil {
		return err
	}
	for {
		switch b, err := m.Next(); err {
		case nil:
			if err := f.Forward(b); err != nil {
				return err
			}
		case ErrDone:
			return nil
		default:
			return err
		}
	}
	return nil
}

func runMerge(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	addrs := cmd.Flag.Args()
	f, err := Forward(addrs[1:]...)
	if err != nil {
		return err
	}
	m, err := Merge(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	for {
		switch b, err := m.Next(); err {
		case nil:
			if err := f.Forward(b); err != nil {
				return err
			}
		case ErrDone:
			return nil
		default:
			return err
		}
	}
	return nil
}

func decodeChunk(r io.Reader) (*Chunk, error) {
	c := new(Chunk)
	if _, err := io.ReadFull(r, c.Sum[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r, c.IP[:]); err != nil {
		return nil, err
	}
	binary.Read(r, binary.BigEndian, &c.Src)
	binary.Read(r, binary.BigEndian, &c.Dst)
	binary.Read(r, binary.BigEndian, &c.Id)
	binary.Read(r, binary.BigEndian, &c.Frag)
	binary.Read(r, binary.BigEndian, &c.Count)
	binary.Read(r, binary.BigEndian, &c.Length)

	c.Payload = make([]byte, int(c.Length))
	if _, err := io.ReadFull(r, c.Payload); err != nil {
		return nil, err
	}
	var s uint32
	binary.Read(r, binary.BigEndian, &s)
	if a := adler32.Checksum(c.Payload); s != a {
		return nil, fmt.Errorf("crc mismatched: %d != %d", s, a)
	}
	binary.Read(r, binary.BigEndian, &c.Roll)
	return c, nil
}
