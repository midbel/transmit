package main

import (
	"bufio"
	"encoding/hex"
	"io"
	"io/ioutil"
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
	dump := cmd.Flag.Bool("x", false, "hexdump")
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
		Buffer int     `toml:"buffer"`
		Routes []route `toml:"route"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}

	r, err := listen(c.Addr)
	if err != nil {
		return err
	}
	var b []byte
	if c.Buffer > 0 {
		b = make([]byte, c.Buffer)
	}

	if *dump {
		r = io.TeeReader(r, hex.Dumper(os.Stdout))
	}
	_, err = io.CopyBuffer(ioutil.Discard, r, b)
	return err
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
	return bufio.NewReader(pr), nil
}
