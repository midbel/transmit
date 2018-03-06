package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
	"github.com/midbel/transmit"
)

var gateway = &cli.Command{
	Run:   runGateway,
	Usage: "gateway <config.toml>",
	Short: "",
	Alias: []string{"recv", "listen", "gw"},
	Desc:  ``,
}

type route struct {
	Proto string   `toml:"proto"`
	Addr  string   `toml:"address"`
	Ports []uint16 `toml:"port"`
}

type conn struct {
	addr  net.Addr
	inner net.Conn

	ports []uint16
}

func (c *conn) Transmit(p *transmit.Packet) error {
	if len(c.ports) > 0 {
		ix := sort.Search(len(c.ports), func(i int) bool {
			return c.ports[i] <= p.Port
		})
		if ix >= len(c.ports) || c.ports[ix] != p.Port {
			return fmt.Errorf("not found", c.ports, p.Port)
		}
	}
	var err error
	if c.inner == nil {
		c.inner, err = net.Dial(c.addr.Network(), c.addr.String())
		if err != nil {
			return err
		}
	}
	switch i := c.inner.(type) {
	case *net.TCPConn:
		_, err = i.Write(transmit.EncodePacket(p))
	case *net.UDPConn:
		_, err = i.Write(p.Payload)
	}
	if err != nil {
		if err, ok := err.(net.Error); ok && !err.Temporary() {
			c.inner.Close()
			c.inner = nil
		}
	}
	return err
}

type pool struct {
	cs []*conn
}

func (p *pool) Close() error {
	var err error
	for _, c := range p.cs {
		if c.inner == nil {
			continue
		}
		if e := c.inner.Close(); e != nil {
			err = e
		}
	}
	return err
}

func (p *pool) Write(bs []byte) (int, error) {
	r := bufio.NewReader(bytes.NewReader(bs))
	k, err := transmit.DecodePacket(r)
	if err == nil {
		for _, c := range p.cs {
			if err := c.Transmit(k); err != nil {
				log.Println(err)
			}
		}
	}
	return len(bs), nil
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
		Addr   string  `toml:"address"`
		Proto  string  `toml:"proto"`
		Debug  bool    `toml:"dump"`
		Buffer int     `toml:"buffer"`
		Cert   cert    `toml:"certificate"`
		Routes []route `toml:"route"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	w, err := prepare(c.Routes, c.Debug)
	if err != nil {
		return err
	}

	r, err := listen(c.Addr, c.Cert.Server())
	if err != nil {
		return err
	}
	if c.Buffer > 0 {
		b := make([]byte, c.Buffer)
		_, err = io.CopyBuffer(w, r, b)
	} else {
		_, err = io.Copy(w, r)
	}
	return nil
}

func prepare(rs []route, d bool) (io.Writer, error) {
	p := new(pool)
	for _, r := range rs {
		var (
			a   net.Addr
			err error
		)
		switch r.Proto {
		case "", "udp":
			a, err = net.ResolveUDPAddr("udp", r.Addr)
		case "tcp":
			a, err = net.ResolveTCPAddr("tcp", r.Addr)
		default:
			return nil, fmt.Errorf("unsupported protocol: %s", r.Proto)
		}
		if err != nil {
			return nil, err
		}
		p.cs = append(p.cs, &conn{addr: a})
	}
	var w io.Writer = p
	if d {
		w = io.MultiWriter(p, hex.Dumper(os.Stdout))
	}

	return w, nil
}

func listen(a string, c *tls.Config) (io.Reader, error) {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}
	if c != nil {
		s = tls.NewListener(s, c)
	}
	pr, pw := io.Pipe()
	go func() {
		defer func() {
			s.Close()
			pr.Close()
			pw.Close()
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
				log.Printf("start receiving packets from %s", c.RemoteAddr())
				_, err := io.Copy(pw, c)
				if err != nil {
					log.Printf("error when receiving packets from %s: %s", c.RemoteAddr(), err)
				}
				log.Printf("done receiving packets from %s", c.RemoteAddr())
			}(c)
		}
	}()
	return pr, nil
}
