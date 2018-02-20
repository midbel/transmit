package main

import (
	"bufio"
	"crypto/tls"
	"log"
	"net"
	"os"
	"sort"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
	"github.com/midbel/transmit"
)

type Proxy struct {
	net.Addr
	conn net.Conn
}

func (p *Proxy) Set(v string) error {
	a, err := net.ResolveTCPAddr("tcp", v)
	if err != nil {
		return err
	}
	p.Addr = a
	return nil
}

func (p *Proxy) Write(bs []byte) (int, error) {
	if p.conn == nil {
		c, err := net.Dial(p.Addr.Network(), p.Addr.String())
		if err != nil {
			return len(bs), nil
		}
		p.conn = c
	}
	_, err := p.conn.Write(bs)
	if err != nil {
		p.conn.Close()
		p.conn = nil
	}
	return len(bs), nil
}

type Group struct {
	net.Addr
	conn net.Conn
}

func (g *Group) Set(v string) error {
	a, err := net.ResolveUDPAddr("udp", v)
	if err != nil {
		return err
	}
	g.Addr = a
	return nil
}

func (g *Group) Write(bs []byte) (int, error) {
	if g.conn == nil {
		c, err := net.Dial(g.Addr.Network(), g.Addr.String())
		if err != nil {
			return len(bs), nil
		}
		g.conn = c
	}
	_, err := g.conn.Write(bs)
	if err != nil {
		g.conn.Close()
		g.conn = nil
	}
	return len(bs), nil
}

type dispatcher struct {
	Port  uint16 `toml:"port"`
	Group *Group `toml:"group"`
	Proxy *Proxy `toml:"proxy"`
}

func (d *dispatcher) Transmit(p *transmit.Packet) error {
	var err error
	if d.Group != nil {
		if _, e := d.Group.Write(p.Payload); err == nil && e != nil {
			err = e
		}
	}
	if d.Proxy != nil {
		if _, e := d.Proxy.Write(transmit.EncodePacket(p)); err == nil && e != nil {
			err = e
		}
	}
	return err
}

var gateway = &cli.Command{
	Run:   runGateway,
	Usage: "gateway <config.toml>",
	Short: "",
	Alias: []string{"recv", "listen", "gw"},
	Desc:  ``,
}

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
		Addr        string       `toml:"address"`
		Cert        cert         `toml:"certificate"`
		Dispatchers []dispatcher `toml:"route"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	sort.Slice(c.Dispatchers, func(i, j int) bool {
		return c.Dispatchers[i].Port < c.Dispatchers[j].Port
	})
	queue, err := listen(c.Addr, c.Cert.Server())
	if err != nil {
		return err
	}
	for p := range queue {
		ix := sort.Search(len(c.Dispatchers), func(i int) bool {
			return c.Dispatchers[i].Port >= p.Port
		})
		if ix >= len(c.Dispatchers) || c.Dispatchers[ix].Port != p.Port {
			continue
		}
		if err := c.Dispatchers[ix].Transmit(p); err != nil {
			log.Printf("fail to transmit packet: %s", err)
		}
	}
	return nil
}

func listen(a string, c *tls.Config) (<-chan *transmit.Packet, error) {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}
	if c != nil {
		s = tls.NewListener(s, c)
	}
	q := make(chan *transmit.Packet)
	go func() {
		defer func() {
			close(q)
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

				log.Printf("start reading packets from %s", c.RemoteAddr())
				defer log.Printf("done reading packets from %s", c.RemoteAddr())
				for i, r := 1, bufio.NewReader(c); ; i++ {
					p, err := transmit.DecodePacket(r)
					switch err {
					case nil:
						go log.Printf("%06d packet received from %s (%d bytes - %x)", i, c.RemoteAddr(), p.Length, p.Sum)
						q <- p
					case transmit.ErrCorrupted:
					default:
						return
					}
				}
			}(c)
		}
	}()
	return q, nil
}
