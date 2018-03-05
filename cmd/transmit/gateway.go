package main

import (
	"encoding/hex"
	"io"
	"log"
	"net"
	"os"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
	"github.com/midbel/transmit"
)

type Packet struct {
	*transmit.Packet
	net.Addr
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
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	r, err := listen(c.Addr)
	if err != nil {
		return err
	}
	w := hex.Dumper(os.Stdout)
	_, err = io.Copy(w, r)

	return nil
}

func listen(a string) (io.Reader, error) {
	s, err := net.Listen("tcp", a)
	if err != nil {
		return nil, err
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
