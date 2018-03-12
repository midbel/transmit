package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"io"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/midbel/cli"
	"github.com/midbel/transmit"
)

type key struct {
	Sum  [md5.Size]byte
	Id   uint16
	when time.Time
}

func (k key) Equal(s []byte) bool {
	return bytes.Equal(k.Sum[:], s)
}

type chunk struct {
	key
	Frag    uint16
	Block   uint16
	Length  uint16
	Payload []byte
}

type buffer struct {
	mu     sync.Mutex
	chunks map[key][]*chunk
}

func (b *buffer) clean() {
	t := time.NewTicker(time.Minute * 5)
	defer t.Stop()
	for w := range t.C {
		b.mu.Lock()
		for k := range b.chunks {
			if w.Sub(k.when) <= time.Minute {
				continue
			}
			delete(b.chunks, k)
		}
		b.mu.Unlock()
	}
}

func (b *buffer) Append(c *chunk) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	cs := append(b.chunks[c.key], c)
	if len(cs) >= int(c.Block) {
		delete(b.chunks, c.key)

		sort.Slice(cs, func(i, j int) bool {
			return cs[i].Frag <= cs[j].Frag
		})
		buf := new(bytes.Buffer)
		sum := md5.New()

		w := io.MultiWriter(sum, buf)
		for i := range cs {
			w.Write(cs[i].Payload)
		}
		if c.Equal(sum.Sum(nil)) {
			return nil
		}
		return buf.Bytes()
	}
	b.chunks[c.key] = cs
	return nil
}

func NewBuffer() *buffer {
	b := &buffer{chunks: make(map[key][]*chunk)}
	go b.clean()

	return b
}

func runReassemble(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	r, err := Listen(cmd.Flag.Arg(0), nil)
	if err != nil {
		return err
	}
	w, err := transmit.Proxy(cmd.Flag.Arg(1), nil)
	b := NewBuffer()
	for c := range Decode(r) {
		bs := b.Append(c)
		if bs == nil {
			continue
		}
		if _, err := w.Write(bs); err != nil {
			log.Println(err)
		}
	}

	return nil
}

func Decode(r io.Reader) <-chan *chunk {
	q := make(chan *chunk, 1024)
	go func() {
		defer close(q)
		for {
			var c chunk
			if _, err := r.Read(c.key.Sum[:]); err != nil {
				return
			}
			c.key.when = time.Now()

			binary.Read(r, binary.BigEndian, &c.key.Id)
			binary.Read(r, binary.BigEndian, &c.Frag)
			binary.Read(r, binary.BigEndian, &c.Block)
			binary.Read(r, binary.BigEndian, &c.Length)
			c.Payload = make([]byte, int(c.Length))
			if _, err := io.ReadFull(r, c.Payload); err != nil {
				return
			}
			q <- &c
		}
	}()
	return q
}

func Listen(a string, c *tls.Config) (io.Reader, error) {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	go func() {
		defer func() {
			pw.Close()
			pr.Close()
			s.Close()
		}()
		for {
			c, err := s.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(pw, c)
			}(c)
		}
	}()
	return bufio.NewReader(pr), nil
}
