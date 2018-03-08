package main

import (
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
	"github.com/midbel/transmit"
)

type group struct {
	Port uint16 `toml:"port"`
	Addr string `toml:"group"`
	Eth  string `toml:"interface"`
}

func (g *group) Dial() (net.Conn, error) {
	return transmit.Subscribe(g.Addr, g.Eth, g.Port)
}

func runRelay(cmd *cli.Command, args []string) error {
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
		Groups []group `toml:"route"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	w, err := transmit.Proxy(c.Addr)
	if err != nil {
		return err
	}
	defer w.Close()

	var wg sync.WaitGroup
	for i := range c.Groups {
		r, err := c.Groups[i].Dial()
		if err != nil {
			log.Println("fail to subscribe to %s: %s", c.Groups[i].Addr, err)
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			io.Copy(w, c)
		}(r)
	}
	wg.Wait()
	return nil
}
