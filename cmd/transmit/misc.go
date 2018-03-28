package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	"github.com/midbel/cli"
	"github.com/midbel/rustine/rw"
	"github.com/midbel/transmit"
)

const DefaultSize = 1024

func runDumper(cmd *cli.Command, args []string) error {
	// file := cmd.Flag.String("w", "", "file")
	debug := cmd.Flag.Bool("x", false, "hexdump")
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
		if *debug {
			go io.Copy(w, c)
		} else {
			go dumpPackets(c)
		}
	}
	return nil
}

func dumpPackets(c net.Conn) {
	defer func() {
		c.Close()
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	var (
		size int64
		seq  uint32
		port uint16
		bs   []byte
	)
	w := time.Now()
	var total, count uint64
	for n := time.Now(); ; n = time.Now() {
		binary.Read(c, binary.BigEndian, &size)
		binary.Read(c, binary.BigEndian, &seq)
		binary.Read(c, binary.BigEndian, &port)
		bs = make([]byte, int(size))
		if n, err := io.ReadFull(c, bs); err != nil || n == 0 {
			break
		}
		total += uint64(size) + 14
		count++
		log.Printf("%24s | %9d | %6d | %6d | %x | %16s | %16s", c.RemoteAddr(), size, seq+1, port, md5.Sum(bs), time.Since(n), time.Since(w))
	}
	elapsed := time.Since(w)
	volume := float64(total) / 1024
	log.Printf("%d packets read %s (%.2fKB, %.2f Mbps)", count, elapsed, volume, ((volume/1024)*8)/elapsed.Seconds())
}

func runSimulate(cmd *cli.Command, args []string) error {
	var (
		size cli.MultiSize
		rate cli.Size
	)
	cmd.Flag.Var(&size, "s", "write packets of size byts to group")
	cmd.Flag.Var(&rate, "t", "rate limiting")
	cmd.Flag.BoolVar(&size.Alea, "r", false, "write packets of random size with upper limit set to to size")
	every := cmd.Flag.Duration("e", time.Second, "write a packet every given elapsed interval")
	count := cmd.Flag.Int("c", 0, "write count packets to group then exit")
	quiet := cmd.Flag.Bool("q", false, "suppress write debug information on stderr")
	zero := cmd.Flag.Bool("z", false, "fill packets only with zero")
	proto := cmd.Flag.String("p", "tcp", "protocol")
	syst := cmd.Flag.Bool("y", false, "system clock")

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
			defer c.Close()
			if c, ok := c.(*net.TCPConn); ok {
				c.SetNoDelay(false)
			}
			var clock transmit.Clock
			if *syst {
				clock = transmit.SystemClock()
			} else {
				clock = transmit.RealClock()
			}
			var writer io.Writer = c
			if r := rate.Float(); r > 0 {
				b := ratelimit.NewBucketWithRateAndClock(r, int64(r), clock)
				writer = ratelimit.Writer(writer, b)
			}
			var reader io.Reader
			if *zero {
				reader = rw.Zero(int(size.Int()))
			} else {
				reader = rw.Rand()
			}
			log.Printf("start writing packets to %s", c.RemoteAddr())

			var sum int64
			s, n := md5.New(), time.Now()
			r, buf := io.TeeReader(reader, s), new(bytes.Buffer)
			for i := 0; *count <= 0 || i < *count; i++ {
				z := size.Int()
				time.Sleep(*every)
				binary.Write(buf, binary.BigEndian, int64(z))
				binary.Write(buf, binary.BigEndian, uint32(i))
				binary.Write(buf, binary.BigEndian, uint16(0))
				if _, err := io.CopyN(buf, r, z); err != nil {
					break
				}
				w := time.Now()
				if _, err := io.Copy(writer, buf); err != nil {
					break
				}
				sum += z
				log.Printf("%24s | %9d | %6d | %6d | %x | %16s | %16s", c.RemoteAddr(), z, i+1, 0, s.Sum(nil), time.Since(w), time.Since(n))
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
