package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/adler32"
	"io"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/midbel/cli"
	"github.com/midbel/transmit"
)

var (
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
	mu     sync.Mutex
	chunks map[Key][]*Chunk
	when   map[Key]time.Time

	sum  hash.Hash
	roll hash.Hash32
}

func Merge() *merger {
	return &merger{
		chunks: make(map[Key][]*Chunk),
		when:   make(map[Key]time.Time),
		sum:    md5.New(),
		roll:   adler32.New(),
	}
}

func (m *merger) Merge(c *Chunk) (*Chunk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cs := append(m.chunks[c.Key], c)
	if len(cs) >= int(c.Count) {
		delete(m.chunks, c.Key)
		delete(m.when, c.Key)

		sort.Slice(cs, func(i, j int) bool {
			return cs[i].Frag <= cs[j].Frag
		})
		m.roll.Reset()
		m.sum.Reset()

		buf := new(bytes.Buffer)
		w := io.MultiWriter(buf, m.sum, m.roll)

		for _, i := range cs {
			w.Write(i.Payload)
			if s := m.roll.Sum32(); s != i.Roll {
				return nil, ErrRollSum
			}
		}
		c.Roll = cs[len(cs)-1].Roll
		if s := m.sum.Sum(nil); !c.Key.Equal(s) {
			return nil, ErrChecksum
		}
		c.Length, c.Frag = uint16(buf.Len()), 0
		c.Payload = buf.Bytes()
		return c, nil
	}
	m.when[c.Key], m.chunks[c.Key] = time.Now(), cs
	return nil, nil
}

func runMerge(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	ws := make(map[uint16]net.Addr)
	cs := make(map[interface{}]net.Conn)
	for i := 1; i < cmd.Flag.NArg(); i++ {
		c, err := transmit.Proxy(cmd.Flag.Arg(i), nil)
		if err != nil {
			return err
		}
		defer c.Close()
		a := c.RemoteAddr().(*net.TCPAddr)
		ws[uint16(a.Port)] = a
	}
	queue, err := listenAndMerge(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	m := Merge()
	when := make(map[Key]time.Time)
	dtstamp := time.Now()
	for c := range queue {
		if _, ok := when[c.Key]; !ok {
			when[c.Key] = time.Now()
		}
		k, err := m.Merge(c)
		if err != nil {
			continue
		}
		if k == nil {
			continue
		}
		log.Printf("%6d | %6d | %9d | %x | %16s | %16s", k.Id, k.Count, len(c.Payload), k.Sum, time.Since(when[c.Key]), time.Since(dtstamp))
		delete(when, c.Key)
		if a, ok := ws[k.Key.Dst]; ok {
			n := k.Route()
			w, ok := cs[n]
			if !ok {
				c, err := net.Dial(a.Network(), a.String())
				if err != nil {
					continue
				}
				cs[n] = c
				w = c
			}
			if _, err := w.Write(k.Payload); err != nil {
				log.Println(err)
				w.Close()
				delete(cs, n)
			}
		}
	}
	return nil
}

func listenAndMerge(a string) (<-chan *Chunk, error) {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}

	queue := make(chan *Chunk, 1000)
	go func() {
		defer func() {
			close(queue)
			s.Close()
		}()
		for {
			c, err := s.Accept()
			if err != nil {
				return
			}
			if c, ok := c.(*net.TCPConn); ok {
				c.SetKeepAlive(true)
			}
			go func(c net.Conn) {
				defer c.Close()
				for {
					c, err := decodeChunk(c)
					if err, ok := err.(net.Error); ok && !err.Temporary() {
						return
					}
					if err != nil {
						return
					}
					queue <- c
				}
			}(c)
		}
	}()
	return queue, nil
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
