package main

import (
	"net"

	"github.com/midbel/cli"
)

var gateway = &cli.Command{
	Usage: "gateway <config.toml>",
	Alias: []string{"recv", "listen", "gw"},
	Run:   runGateway,
}

func runGateway(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	return nil
}

func listen(a, c string) (<-chan net.Conn, error) {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
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
