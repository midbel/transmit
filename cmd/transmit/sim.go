package main

import (
	"crypto/md5"
	"crypto/rand"
	"io"
	"io/ioutil"
	"log"
	random "math/rand"
	"net"
	"sync"
	"time"

	"github.com/midbel/cli"
)

const DefaultSize = 1024

func runSimulate(cmd *cli.Command, args []string) error {
	every := cmd.Flag.Duration("e", time.Second, "write a packet every given elapsed interval")
	count := cmd.Flag.Int("c", 0, "write count packets to group then exit")
	size := cmd.Flag.Int("s", 1024, "write packets of size byts to group")
	alea := cmd.Flag.Bool("r", false, "write packets of random size with upper limit set to to size")
	quiet := cmd.Flag.Bool("q", false, "suppress write debug information on stderr")

	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}

	if *quiet {
		log.SetOutput(ioutil.Discard)
	}

	var wg sync.WaitGroup
	for _, g := range cmd.Flag.Args() {
		c, err := net.Dial("udp", g)
		if err != nil {
			log.Printf("fail to subscribe to %s: %s", g, err)
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			log.Printf("start writing packets to %s", c.RemoteAddr())
			s := md5.New()
			r := io.TeeReader(rand.Reader, s)
			for i := 0; *count <= 0 || i < *count; i++ {
				z := int64(*size)
				if *alea {
					z = random.Int63n(z)
				}
				n, err := io.CopyN(c, r, z)
				if err != nil {
					break
				}
				log.Printf("%s - %6d - %6d - %x", c.RemoteAddr(), i+1, n, s.Sum(nil))
				s.Reset()
				time.Sleep(*every)
			}
			c.Close()
			wg.Done()

			log.Printf("done writing packets to %s", c.RemoteAddr())
		}(c)
	}
	wg.Wait()
	return nil
}
