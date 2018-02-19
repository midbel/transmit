package main

import (
	"crypto/tls"
	"net"
	"os"
	"sync"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

type group struct {

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
		Addr string `toml:"address"`
		Cert cert   `toml:"certificate"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	queue, err := listen(c.Addr, c.Cert.Server())
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for c := range queue {
		_ = c
	}
	wg.Wait()
	return nil
}

func listen(a string, c *tls.Config) (<-chan net.Conn, error) {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
	}
	if c != nil {
		s = tls.NewListener(s, c)
	}
	q := make(chan net.Conn)
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
			q <- c
		}
	}()
	return q, nil
}
