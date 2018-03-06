package main

import (
	"crypto/tls"
	"encoding/hex"
	"io"
	"log"
	"net"
	"os"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
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

	r, err := listen(c.Addr, c.Cert.Server())
	if err != nil {
		return err
	}
	var b []byte
	if c.Buffer > 0 {
		b = make([]byte, c.Buffer)
	}
	w := hex.Dumper(os.Stdout)
	_, err = io.CopyBuffer(w, r, b)
	return nil
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
