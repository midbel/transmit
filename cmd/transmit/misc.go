package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
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
	sum := md5.New()
	r, w := io.TeeReader(c, sum), time.Now()

	var total, count uint64
	for n := time.Now(); ; n = time.Now() {
		binary.Read(r, binary.BigEndian, &size)
		binary.Read(r, binary.BigEndian, &seq)
		binary.Read(r, binary.BigEndian, &port)
		bs = make([]byte, int(size))
		if n, err := io.ReadFull(r, bs); err != nil || n == 0 {
			break
		}
		total += uint64(size) + 14
		count++
		log.Printf("%s | %9d | %6d | %6d | %x | %16s | %16s", c.RemoteAddr(), size, seq, port, sum.Sum(nil), time.Since(n), time.Since(w))
		sum.Reset()
	}
	elapsed := time.Since(w)
	volume := float64(total) / 1024
	log.Printf("%d packets read %s (%.2fKB, %.2f Mbps)", count, elapsed, volume, ((volume/1024)*8)/elapsed.Seconds())
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
				c.SetNoDelay(false)
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
				if *alea {
					z = rand.Int63n(z)
				}
				time.Sleep(*every)
				binary.Write(buf, binary.BigEndian, int64(z))
				binary.Write(buf, binary.BigEndian, uint32(i))
				binary.Write(buf, binary.BigEndian, uint16(0))
				if _, err := io.CopyN(buf, r, z); err != nil {
					break
				}
				if _, err := io.Copy(c, buf); err != nil {
					break
				}
				sum += z
				log.Printf("%s - %6d - %6d - %x", c.RemoteAddr(), i+1, z, s.Sum(nil))
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
