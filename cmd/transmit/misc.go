package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/adler32"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	"github.com/midbel/cli"
	"github.com/midbel/rustine"
	"github.com/midbel/rustine/rw"
	"github.com/midbel/transmit"
)

const DefaultSize = 1024

func runDumper(cmd *cli.Command, args []string) error {
	file := cmd.Flag.String("w", "", "file")
	dump := cmd.Flag.Bool("x", false, "hexdump")
	perf := cmd.Flag.Bool("p", false, "bandwidth")
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
		a, err := c.Accept()
		if err != nil {
			return err
		}
		if c, ok := a.(*net.TCPConn); ok {
			c.SetKeepAlive(true)
		}
		if *dump {
			go io.Copy(w, a)
		} else if *perf {
			go dumpStats(a, *file)
		} else {
			go dumpPackets(a, *file)
		}
	}
	return nil
}

func dumpStats(c net.Conn, f string) {
	const megabits = cli.Mega * 8.0
	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", c.RemoteAddr()), log.Ltime)

	w := new(bytes.Buffer)

	now, total, avg := time.Now(), 0.0, 0.0
	values := make([]float64, 10)
	var r io.Reader
	if f, err := os.Create(filepath.Join(f, rustine.RandomString(8)+".dat")); err == nil {
		defer f.Close()
		r = io.TeeReader(c, f)
	} else {
		r = c
	}
	for i := 1; ; i++ {
		c.SetReadDeadline(time.Now().Add(time.Second))
		n, err := io.Copy(w, r)
		w.Reset()
		if err, ok := err.(net.Error); ok && err.Timeout() {
			size := float64(n) * 8
			total += size

			if i >= len(values) {
				j := (i - len(values)) % len(values)
				avg += (size - values[j]) / float64(i)

				values[j] = size
			} else {
				avg = 0.0
				for _, v := range values {
					avg += v
				}
				avg /= float64(len(values))
				values[(i-1)%len(values)] = size
			}
			elapsed := time.Since(now)

			logger.Printf("%18s | %6d | %9.2fMbps | %9.2fMbps | %9.2fMbps",
				elapsed,
				i,
				size/megabits,
				(total/megabits)/elapsed.Seconds(),
				avg/megabits,
			)
		}
	}
}

func dumpPackets(c net.Conn, f string) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
		c.Close()
	}()
	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", c.RemoteAddr()), log.Ltime)

	var (
		size  int64
		port  uint16
		seq   uint32
		crc   uint32
		total uint64
		count uint64
	)
	w, roll := time.Now(), adler32.New()

	var r io.Reader
	if f, err := os.Create(filepath.Join(f, rustine.RandomString(8)+".dat")); err == nil {
		defer f.Close()
		r = io.TeeReader(c, io.MultiWriter(f, roll))
	} else {
		r = io.TeeReader(c, roll)
	}

	for n := time.Now(); ; n = time.Now() {
		binary.Read(r, binary.BigEndian, &size)
		binary.Read(r, binary.BigEndian, &seq)
		binary.Read(r, binary.BigEndian, &port)

		bs := make([]byte, int(size))
		if n, err := io.ReadFull(r, bs); err != nil || n == 0 {
			break
		}
		got := roll.Sum32()
		binary.Read(r, binary.BigEndian, &crc)
		if crc != got {
			return
		}
		total += uint64(size) + 14
		count++
		logger.Printf("%9d | %6d | %6d | %x | %16s | %16s | %08x", size, seq+1, port, md5.Sum(bs), time.Since(n), time.Since(w), crc)
		roll.Reset()
	}
	elapsed := time.Since(w)
	volume := float64(total) / 1024
	logger.Printf("%d packets read %s (%.2fKB, %.2f Mbps)", count, elapsed, volume, ((volume/1024)*8)/elapsed.Seconds())
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
				reader = rw.Zero(int(size.Sum()))
			} else {
				reader = rw.Rand()
			}
			logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", c.RemoteAddr()), log.Ltime)
			logger.Printf("start writing packets to %s", c.RemoteAddr())

			var sum int64
			s, n := md5.New(), time.Now()
			r, buf := io.TeeReader(reader, s), new(bytes.Buffer)
			for i := 0; *count <= 0 || i < *count; i++ {
				time.Sleep(*every)

				z := size.Int()
				binary.Write(buf, binary.BigEndian, int64(z))
				binary.Write(buf, binary.BigEndian, uint32(i))
				binary.Write(buf, binary.BigEndian, uint16(0))
				if _, err := io.CopyN(buf, r, z); err != nil {
					break
				}
				crc := adler32.Checksum(buf.Bytes())
				binary.Write(buf, binary.BigEndian, crc)

				w := time.Now()
				if _, err := io.Copy(writer, buf); err != nil {
					break
				}
				sum += z
				logger.Printf("%9d | %6d | %6d | %x | %16s | %16s | %08x", z, i+1, 0, s.Sum(nil), time.Since(w), time.Since(n), crc)
				s.Reset()
				buf.Reset()
			}
			c.Close()
			wg.Done()
			logger.Printf("%d bytes written in %s to %s", sum, time.Since(n), c.RemoteAddr())
		}(c)
	}
	wg.Wait()
	return nil
}
