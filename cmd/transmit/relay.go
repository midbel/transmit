package main

import (
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/juju/ratelimit"
	"github.com/midbel/cli"
	"github.com/midbel/toml"
	"github.com/midbel/transmit"
)

type bandwidth struct {
	Rate float64 `toml:"rate"`
	Cap  int64   `toml:"capacity"`
}

func (b bandwidth) Writer(w io.Writer) io.Writer {
	if b.Rate == 0 {
		return w
	}
	b.Rate *= 1024 * 1024
	if b.Cap == 0 {
		b.Cap = int64(b.Rate) * 4
	}
	k := ratelimit.NewBucketWithRate(b.Rate, b.Cap)
	return ratelimit.Writer(w, k)
}

type group struct {
	Port   uint16 `toml:"port"`
	Addr   string `toml:"group"`
	Eth    string `toml:"interface"`
	Buffer uint16 `toml:"buffer"`
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
		Addr   string    `toml:"address"`
		Usage  bandwidth `toml:"bandwidth"`
		Cert   cert      `toml:"certificate"`
		Groups []group   `toml:"route"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}

	x, err := transmit.Proxy(c.Addr, c.Cert.Client())
	if err != nil {
		return err
	}
	defer x.Close()

	w := c.Usage.Writer(x)

	var wg sync.WaitGroup
	for i := range c.Groups {
		r, err := c.Groups[i].Dial()
		if err != nil {
			log.Println("fail to subscribe to %s: %s", c.Groups[i].Addr, err)
			continue
		}
		wg.Add(1)
		go func(c net.Conn, s int) {
			defer wg.Done()
			var b []byte
			if s > 0 {
				b = make([]byte, s)
			}
			io.CopyBuffer(w, c, b)
		}(r, int(c.Groups[i].Buffer))
	}
	wg.Wait()
	return nil
}
