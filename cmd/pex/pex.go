package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const DefaultBufferSize = 1024

var (
	ErrPoolClosed = errors.New("pool closed")
	ErrInvalidTag = errors.New("invalid tag")
)

type pool struct {
	mu     sync.Mutex
	urls   map[byte]url.URL
	conns  map[byte]chan net.Conn
	closed bool
}

func New(urls []string, size int) (*pool, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid size")
	}
	infos := make(map[byte]url.URL)
	conns := make(map[byte]chan net.Conn)

	for _, u := range urls {
		uri, err := url.Parse(u)
		if err != nil {
			return nil, err
		}
		raw, err := hex.DecodeString(uri.Query().Get("tag"))
		if err != nil || len(raw) == 0 {
			return nil, ErrInvalidTag
		}
		tag := raw[0]
		infos[tag] = *uri
		conns[tag] = make(chan net.Conn, size)
	}
	return &pool{urls: infos, conns: conns}, nil
}

func (p *pool) Acquire(t byte) (net.Conn, error) {
	if q, ok := p.conns[t]; ok {
		var client net.Conn
		select {
		case c, ok := <-q:
			if !ok {
				return nil, ErrPoolClosed
			}
			client = c
		default:
			uri := p.urls[t]
			c, err := net.Dial(uri.Scheme, uri.Host)
			if err != nil {
				return nil, err
			}
			client = c
		}
		return client, nil
	}
	return nil, ErrInvalidTag
}

func (p *pool) Release(t byte, c net.Conn) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return c.Close()
	}

	if q, ok := p.conns[t]; ok {
		var err error
		select {
		case q <- c:
			break
		default:
			err = c.Close()
		}
		return err
	}
	return ErrInvalidTag
}

func (p *pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	p.closed = true

	var err error
	for t, q := range p.conns {
		close(q)
		for c := range q {
			err = c.Close()
		}
		delete(p.conns, t)
		delete(p.urls, t)
	}
	return err
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [-c] [-d] [-v] [-s] <addr> <remote>\n", os.Args[0])
		os.Exit(1)
	}
}

func main() {
	config := struct {
		Verbose bool
		Size    int
		Count   int
		Datadir string
	}{}
	flag.StringVar(&config.Datadir, "d", config.Datadir, "data dir")
	flag.BoolVar(&config.Verbose, "v", config.Verbose, "verbose")
	flag.IntVar(&config.Size, "s", config.Size, "size")
	flag.IntVar(&config.Count, "c", config.Count, "count")
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
		queue, err := accept(flag.Arg(0), config.Size)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		p, err := New(flag.Args()[1:], config.Count)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer p.Close()
		
		var counter uint64
	Loop:
		for buf := range queue {
			if len(buf) == 0 {
				continue
			}
			c, err := p.Acquire(buf[0])
			counter++
			if config.Verbose {
				go func(b []byte, c uint64) {
					fmt.Fprintf(os.Stderr, "%s | %8d | %8d | %x\n", time.Now().Format(time.RFC3339), c, len(b), md5.Sum(b))
				}(buf, counter)
			}
			switch {
			case err == ErrPoolClosed:
				break Loop
			case err == ErrInvalidTag && config.Datadir != "":
				go func(datadir string, chunk []byte) {
					if len(chunk) == 0 {
						return
					}
					t := time.Now().Format("20060102_150405")
					f, err := os.Create(filepath.Join(datadir, t + ".dat"))
					if err != nil {
						return
					}
					defer f.Close()
					f.Write(chunk)
				}(config.Datadir, buf[:])
			case err == nil:
				if _, err := c.Write(buf); err == nil {
					p.Release(buf[0], c)
				}
			default:
				continue
			}

		}
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
