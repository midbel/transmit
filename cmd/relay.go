package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/midbel/rustine/cli"
	"github.com/midbel/rustine/cli/toml"
)

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
		Channels []Channel `toml:"channels"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(len(c.Channels))
	for i := range c.Channels {
		if c.Channels[i].Address == "" {
			c.Channels[i].Address = c.Addr
		}
		go func(c Channel) {
			defer wg.Done()
			if err := c.Run(); err != nil {
				log.Println(err)
			}
		}(c.Channels[i])
	}
	wg.Wait()

	return nil
}

type Channel struct {
	Restart uint8  `toml:"restart"`
	Port    uint16 `toml:"port"`
	Group   string `toml:"group"`
	Address string `toml:"address"`
	Ifi     string `toml:"interface"`

	reader net.Conn
	writer net.Conn
	err    error
}

func (c *Channel) Run() error {
	if c.Group == "" {
		return fmt.Errorf("no group configured")
	}
	for i, r := 0, int(c.Restart); ; i++ {
		if c.Restart > 0 && i >= r {
			break
		}
		delay := time.Second * time.Duration(i)
		if s := time.Second * 10; delay >= s {
			delay = s
		}
		if c.reader, c.err = Subscribe(c.Group, c.Ifi); c.err != nil {
			<-time.After(delay)
			continue
		}
		if c.writer, c.err = Forward(c.Address, c.Port); c.err != nil {
			<-time.After(delay)
			continue
		}
		c.copy()
	}
	return c.err
}

func (c *Channel) copy() {
	defer func() {
		c.reader.Close()
		c.writer.Close()
	}()
	for {
		_, c.err = io.Copy(c.writer, c.reader)
		if err, ok := c.err.(net.Error); ok && !err.Temporary() {
			break
		}
	}
}

type forwarder struct {
	net.Conn

	buf   *bytes.Buffer
	port  uint16
	count uint32
}

func (f *forwarder) Write(bs []byte) (int, error) {
	defer func() {
		atomic.AddUint32(&f.count, 1)
	}()
	binary.Write(f.buf, binary.BigEndian, atomic.LoadUint32(&f.count))
	binary.Write(f.buf, binary.BigEndian, f.port)
	binary.Write(f.buf, binary.BigEndian, uint16(0))
	binary.Write(f.buf, binary.BigEndian, uint16(len(bs)))
	f.buf.Write(bs)

	if w, err := io.Copy(f.Conn, f.buf); err != nil {
		return int(w), err
	}
	return len(bs), nil
}

func Forward(a string, p uint16) (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", a)
	if err != nil {
		return nil, err
	}
	c, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	if p == 0 {
		p = uint16(addr.Port)
	}
	f := &forwarder{
		Conn: c,
		buf:  new(bytes.Buffer),
		port: p,
	}
	return f, nil
}

func Subscribe(a, i string) (net.Conn, error) {
	addr, err := net.ResolveUDPAddr("udp", a)
	if err != nil {
		return nil, err
	}
	var ifi *net.Interface
	if i, err := net.InterfaceByName(i); err == nil {
		ifi = i
	}
	return net.ListenMulticastUDP("udp", ifi, addr)
}
