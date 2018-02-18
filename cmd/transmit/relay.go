package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
	"github.com/midbel/transmit"
)

var relay = &cli.Command{
	Run:   runRelay,
	Usage: "relay <relay.toml>",
	Short: "",
	Alias: []string{"send"},
	Desc:  ``,
}

type relayer struct {
	Addr  string `toml:"address"`
	Group string `toml:"group"`
	Port  int    `toml:"port"`
}

func (r relayer) Transmit(e time.Duration, c *tls.Config) error {
	prefix := fmt.Sprintf("[%s-%d] ", r.Group, r.Port)
	logger := log.New(os.Stderr, prefix, log.LstdFlags)
	for {
		logger.Printf("start transmitting packets from %s to %s", r.Group, r.Addr)
		if err := r.copy(c); err != nil {
			logger.Printf("fail to copy packets from %s to %s: %s", r.Group, r.Addr, err)
		}
		logger.Printf("done transmitting packets from %s to %s", r.Group, r.Addr)
		time.Sleep(e)
	}
	return nil
}

func (r relayer) copy(c *tls.Config) error {
	w, err := transmit.Forward(r.Addr, r.Port, c)
	if err != nil {
		return fmt.Errorf("fail to connect to %s: %s", r.Addr, err)
	}
	defer w.Close()

	g, err := transmit.Subscribe(r.Group)
	if err != nil {
		return fmt.Errorf("fail to subscribe to %s: %s", r.Group, err)
	}
	defer g.Close()

	_, err = io.Copy(w, g)
	return err
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
		Addr     string    `toml:"address"`
		Timeout  int       `toml:"timeout"`
		Cert     cert      `toml:"certificate"`
		Relayers []relayer `toml:"route"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	timeout := time.Second
	if c.Timeout > 0 {
		timeout = time.Duration(c.Timeout) * time.Second
	}
	cert := c.Cert.Client()

	var wg sync.WaitGroup
	for i := range c.Relayers {
		if c.Relayers[i].Addr == "" {
			c.Relayers[i].Addr = c.Addr
		}
		go func(r *relayer) {
			if err := r.Transmit(timeout, cert); err != nil {
				log.Println(err)
			}
			wg.Done()
		}(&c.Relayers[i])
		wg.Add(1)
	}
	wg.Wait()
	return nil
}
