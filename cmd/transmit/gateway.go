package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
	"github.com/midbel/transmit"
)

type route struct {
	Port  uint16 `toml:"port"`
	Addr  string `toml:"address"`
	Proto string `toml:"proto"`
	Cert  cert   `toml:"certificate"`
}

func (r *route) Dial() (net.Conn, error) {
	switch r.Proto {
	case "", "udp":
		return net.Dial("udp", r.Addr)
	case "tcp":
		return transmit.Proxy(r.Addr, r.Cert.Client())
	default:
		return nil, fmt.Errorf("unsupported protocol %s", r.Proto)
	}
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
		Cert   cert    `toml:"certificate"`
		Routes []route `toml:"route"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	n := transmit.NewPortMux()
	for i := range c.Routes {
		r, err := c.Routes[i].Dial()
		if err != nil {
			log.Printf("fail to connect to %s: %s", c.Addr, err)
			continue
		}
		defer r.Close()
		n.Register(c.Routes[i].Port, r)
	}
	return transmit.Listen(c.Addr, c.Cert.Server(), n)
}
