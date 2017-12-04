package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
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
	if _, _, err := net.SplitHostPort(c.Mon); err == nil {
		go http.ListenAndServe(c.Mon, nat)
	}
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

type counter struct {
	Addr  addr      `json:"addr"`
	Size  uint64    `json:"size"`
	Count uint64    `json:"count"`
	When  time.Time `json:"dtstamp"`
}

func (c *counter) Update(s int) {
	c.Count++
	c.When = time.Now()
	c.Size += uint64(s)
}

type Nat struct {
	net.Listener

	mu      sync.RWMutex
	proxies map[uint16]net.Conn
	groups  map[uint16]net.Conn
	writers map[addr]io.Writer

	cu       sync.RWMutex
	counters map[addr]*counter
}

func (n *Nat) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.cu.RLock()
	defer n.cu.RUnlock()

	cs := make([]*counter, 0, len(n.counters))
	for _, c := range n.counters {
		cs = append(cs, c)
	}
	json.NewEncoder(w).Encode(cs)
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
	defer n.Listener.Close()
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
		z, err := w.Write(p.Payload)
		n.cu.Lock()
		n.counters[k].Update(z)
		n.cu.Unlock()
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

	n.cu.Lock()
	defer n.cu.Unlock()
	n.counters[k] = &counter{Addr: k}

	z, err := w.Write(p.Payload)
	n.counters[k].Update(z)

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
