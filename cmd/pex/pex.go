package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
)

const DefaultBufferSize = 1024

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s  [-v] [-s] <addr> <remote>\n", os.Args[0])
		os.Exit(1)
	}
}

func main() {
	config := struct {
		Verbose bool
		Size    int
	}{}
	flag.BoolVar(&config.Verbose, "v", config.Verbose, "verbose")
	flag.IntVar(&config.Size, "s", config.Size, "size")
	flag.Parse()

	switch flag.NArg() {
	case 0:
		flag.Usage()
		return
	case 1:
		queue, err := accept(flag.Arg(0), config.Size)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for buf := range queue {
			if len(buf) == 0 {
				continue
			}
			fmt.Fprintln(os.Stdout, hex.Dump(buf))
		}
	default:
	}
}

func accept(addr string, z int) (<-chan []byte, error) {
	uri, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	switch s := strings.ToLower(uri.Scheme); s {
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
		addr = uri.Host
	case "unix":
		addr = uri.Path
	}
	serv, err := net.Listen(uri.Scheme, addr)
	if err != nil {
		return nil, err
	}
	queue := make(chan []byte)
	go func() {
		defer func() {
			close(queue)
			serv.Close()
		}()
		var wg sync.WaitGroup
		for {
			c, err := serv.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go recv(c, queue, z, &wg)
		}
		wg.Wait()
	}()
	return queue, nil
}

func recv(c net.Conn, queue chan []byte, z int, wg *sync.WaitGroup) {
	defer func() {
		c.Close()
		wg.Done()
	}()
	if z <= 0 {
		z = DefaultBufferSize
	}

	var buf bytes.Buffer
	for {
		var abort bool
		for {
			chunk := make([]byte, z)
			r, err := c.Read(chunk)
			switch {
			case err == io.EOF:
				abort = true
			case err != nil:
				buf.Reset()
				break
			}
			buf.Write(chunk[:r])
			if r < z {
				break
			}
		}
		queue <- buf.Bytes()
		if abort {
			return
		}
		buf.Reset()
	}
}
