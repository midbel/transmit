package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/midbel/rustine/cli"
	"github.com/midbel/rustine/cli/toml"
)

func runGateway(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	defer f.Close()

	c := struct {
		Addr   string  `toml:"address"`
		Mon    string  `toml:"monitor"`
		Routes []Route `toml:"routes"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	nat, err := Listen(c.Addr, c.Routes)
	if err != nil {
		return err
	}
	defer nat.Close()
	return nat.Accept()
}

func Listen(a string, rs []Route) (*Nat, error) {
	gs := make(map[uint16]net.Conn)
	for _, r := range rs {
		a, err := net.ResolveUDPAddr("udp", r.Group)
		if err != nil {
			return nil, err
		}
		c, err := net.DialUDP("udp", nil, a)
		if err != nil {
			return nil, err
		}
		p := r.Port
		if p == 0 {
			p = uint16(a.Port)
		}
		gs[p] = c
	}
	s, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}
	n := &Nat{
		Listener: s,
		groups:   gs,
		writers:  make(map[addr]io.Writer),
	}
	return n, nil
}

type addr struct {
	net.Addr
	port uint16
}

type Route struct {
	Port  uint16 `toml:"port"`
	Group string `toml:"group"`
}

type Packet struct {
	Counter uint32
	Port    uint16
	Length  uint16
	Sum     uint16
	Payload []byte
}

type Nat struct {
	net.Listener

	mu      sync.RWMutex
	proxies map[uint16]net.Conn
	groups  map[uint16]net.Conn
	writers map[addr]io.Writer
}

func (n *Nat) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	err := n.Listener.Close()
	for _, c := range n.groups {
		if e := c.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (n *Nat) Accept() error {
	for {
		c, err := n.Listener.Accept()
		if err != nil {
			return err
		}
		if c, ok := c.(*net.TCPConn); ok {
			c.SetKeepAlive(true)
			c.SetKeepAlivePeriod(time.Second * 300)
		}
		go func(c net.Conn) {
			defer c.Close()
			for {
				p, err := decodePacket(c)
				if err != nil {
					log.Println(err)
					return
				}
				if err := n.forward(c.RemoteAddr(), p); err != nil {
					log.Println(err)
				}
			}
		}(c)
	}
	return nil
}

func (n *Nat) forward(a net.Addr, p *Packet) error {
	k := addr{a, p.Port}

	n.mu.RLock()
	if w, ok := n.writers[k]; ok {
		_, err := w.Write(p.Payload)
		return err
	}
	var ws []io.Writer
	if p, ok := n.proxies[p.Port]; ok {
		ws = append(ws, p)
	}
	if g, ok := n.groups[p.Port]; ok {
		ws = append(ws, g)
	}
	n.mu.RUnlock()

	var w io.Writer
	switch len(ws) {
	case 0:
		return fmt.Errorf("no route configured for %s %d", a, p.Port)
	case 1:
		w = ws[0]
	default:
		w = io.MultiWriter(ws...)
	}
	n.mu.Lock()
	n.writers[k] = w
	n.mu.Unlock()

	_, err := w.Write(p.Payload)
	return err
}

func decodePacket(r io.Reader) (*Packet, error) {
	p := new(Packet)

	if err := binary.Read(r, binary.BigEndian, &p.Counter); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Port); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Sum); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &p.Length); err != nil {
		return nil, err
	}
	p.Payload = make([]byte, int(p.Length))
	if _, err := io.ReadFull(r, p.Payload); err != nil {
		return nil, err
	}
	return p, nil
}
