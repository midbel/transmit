package main

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/midbel/cli"
	"github.com/midbel/rustine/rw"
)

const DefaultSize = 1024

func runDumper(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	c, err := net.Listen("tcp", cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	defer c.Close()
	w := hex.Dumper(os.Stdout)
	for {
		c, err := c.Accept()
		if err != nil {
			return err
		}
		if c, ok := c.(*net.TCPConn); ok {
			c.SetKeepAlive(true)
		}
		defer c.Close()
		go io.Copy(w, c)
	}
	return nil
}

func runSimulate(cmd *cli.Command, args []string) error {
	size := cli.Size(4096)
	cmd.Flag.Var(&size, "s", "write packets of size byts to group")
	every := cmd.Flag.Duration("e", time.Second, "write a packet every given elapsed interval")
	count := cmd.Flag.Int("c", 0, "write count packets to group then exit")
	alea := cmd.Flag.Bool("r", false, "write packets of random size with upper limit set to to size")
	quiet := cmd.Flag.Bool("q", false, "suppress write debug information on stderr")
	zero := cmd.Flag.Bool("z", false, "fill packets only with zero")
	proto := cmd.Flag.String("p", "tcp", "protocol")

	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}

	if *quiet {
		log.SetOutput(ioutil.Discard)
	}

	var wg sync.WaitGroup
	for _, g := range cmd.Flag.Args() {
		c, err := net.Dial(*proto, g)
		if err != nil {
			log.Printf("fail to subscribe to %s: %s", g, err)
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			if c, ok := c.(*net.TCPConn); ok {
				c.SetWriteBuffer(128 * 1024)
				c.SetNoDelay(false)
			}
			var reader io.Reader
			if *zero {
				reader = rw.Zero(int(size.Int()))
			} else {
				reader = rw.Rand()
			}
			var sum int64
			n := time.Now()
			log.Printf("start writing packets to %s", c.RemoteAddr())
			s := md5.New()
			r := io.TeeReader(reader, s)
			for i := 0; *count <= 0 || i < *count; i++ {
				z := size.Int()
				if *alea {
					z = rand.Int63n(z)
				}
				time.Sleep(*every)
				n, err := io.CopyN(c, r, z)
				if err != nil {
					break
				}
				sum += n
				log.Printf("%s - %6d - %6d - %x", c.RemoteAddr(), i+1, n, s.Sum(nil))
				s.Reset()
			}
			c.Close()
			log.Printf("%d bytes written in %s to %s", sum, time.Since(n), c.RemoteAddr())

			wg.Done()
		}(c)
	}
	wg.Wait()
	return nil
}
