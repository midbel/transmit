package main

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"
)

const helpText = `{{.Name}} tunnel multicast packets over a TCP connection.

{{.Name}} is a small tool designed to send multicast packets from one network
to another which missed connectivity with the former.

Its original goal is to be able to forward packets from multicast groups
through firewalls that allow only outgoing TCP/TLS connections.

options:
	
	-l: listen for incoming packets
	-c: certificates to encrypt communication between agents
	-k: keep transfered file(s) (default: remove files transfered)
	-w: time to wait when connection failure is encountered
	-s: size of bytes to read/write from connections
	-t: transfer file(s)
	-v: dump packets length + md5 on stderr

arguments:
	
	local: local address to listen for incoming packets
	remote: remote address to forward received packets

`

const DefaultBufferSize = 1024

type conn struct {
	net.Conn
	counter uint64
}

func (c *conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if err != nil {
		return n, err
	}
	go func(b []byte) {
		v := atomic.AddUint64(&c.counter, 1)
		fmt.Fprintf(os.Stderr, "%s | %8d | %8d | %x\n", time.Now().Format(time.RFC3339), v, n, md5.Sum(b))
	}(b[:n])

	return n, err
}

func (c *conn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if err != nil {
		return n, err
	}
	go func(b []byte) {
		v := atomic.AddUint64(&c.counter, 1)
		fmt.Fprintf(os.Stderr, "%s | %8d | %8d | %x\n", time.Now().Format(time.RFC3339), v, n, md5.Sum(b))
	}(b[:n])

	return n, err
}

func init() {
	flag.Usage = func() {
		t := template.New("usage")
		template.Must(t.Parse(helpText))
		data := struct{ Name string }{
			Name: os.Args[0],
		}
		t.Execute(os.Stderr, data)
		fmt.Fprintf(os.Stderr, "usage: %s [-p] [-t] [-k] [-l] [-c] [-w] <local> <remote>\n", os.Args[0])
		return
	}
}

func main() {
	config := struct {
		Listen      bool
		Verbose     bool
		Transfer    bool
		Keep        bool
		Proxy       bool
		Size        int
		Interface   string
		Certificate string
		Wait        time.Duration
	}{}

	flag.IntVar(&config.Size, "s", DefaultBufferSize, "size")
	flag.BoolVar(&config.Verbose, "v", false, "verbose")
	flag.BoolVar(&config.Listen, "l", false, "listen")
	flag.BoolVar(&config.Keep, "k", false, "keep")
	flag.BoolVar(&config.Transfer, "t", false, "transfer")
	flag.BoolVar(&config.Proxy, "p", false, "proxy")
	flag.StringVar(&config.Interface, "i", "eth0", "interface")
	flag.StringVar(&config.Certificate, "c", "", "certificate")
	flag.DurationVar(&config.Wait, "w", time.Second, "")
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	var cfg *tls.Config
	if c := config.Certificate; c != "" {
		pem := filepath.Join(c, "transmit.cert")
		key := filepath.Join(c, "transmit.key")

		if c, err := tls.LoadX509KeyPair(pem, key); err != nil {
			fmt.Fprintf(os.Stderr, "fail to load certificate from %s: %s\n", config.Certificate, err)
			os.Exit(1)
		} else {
			cfg = &tls.Config{Certificates: []tls.Certificate{c}}
		}
	}

	var err error
	switch s, d := flag.Arg(0), flag.Arg(1); {
	case config.Listen:
		err = runGateway(s, d, config.Size, config.Verbose, config.Proxy, cfg)
	case config.Transfer:
		err = runTransfer(s, d, config.Size, config.Keep, cfg)
	default:
		err = runRelay(s, d, config.Interface, config.Size, config.Verbose, config.Wait, cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runGateway(s, d string, z int, v, p bool, c *tls.Config) error {
	uri, err := url.Parse(s)
	if err != nil {
		return err
	}
	serv, err := net.Listen(uri.Scheme, uri.Host)
	if err != nil {
		return err
	}
	defer serv.Close()

	var listener net.Listener
	if c != nil {
		c.ClientAuth = tls.RequireAnyClientCert
		listener = tls.NewListener(serv, c)
	} else {
		listener = serv
	}

	var wg sync.WaitGroup
	for {
		client, err := listener.Accept()
		if err != nil {
			client.Close()
			continue
		}
		group, err := openClient(d, v)
		if err != nil {
			client.Close()
			continue
		}
		wg.Add(1)
		go func(c, g net.Conn) {
			defer wg.Done()
			if p {
				if err := transmit(g, c, z); err != nil {
					return
				}
			} else {
				if err := reassemble(g, c, z); err != nil {
					return
				}
			}
		}(client, group)
	}
	wg.Wait()
	return nil
}

func runTransfer(s, d string, z int, k bool, c *tls.Config) error {
	var client net.Conn
	for i := 0; i < 5; i++ {
		if c, err := openClient(d, false); err != nil {
			time.Sleep(time.Second * time.Duration(i))
			continue
		} else {
			client = c
		}
	}
	if client == nil {
		return fmt.Errorf("connection to %s failed after 5 retries", d)
	}
	if c != nil {
		c.InsecureSkipVerify = true
		client = tls.Client(client, c)
	}
	infos, err := ioutil.ReadDir(s)
	if err != nil {
		return err
	}
	for _, i := range infos {
		f, err := os.Open(filepath.Join(s, i.Name()))
		if err != nil {
			return err
		}
		buf := make([]byte, z)
		if _, err := io.CopyBuffer(client, f, buf); err != nil {
			f.Close()
			return err
		}
		f.Close()
		if !k {
			os.Remove(s)
		}
	}
	return nil
}

func runRelay(s, d, i string, z int, v bool, w time.Duration, c *tls.Config) error {
	for {
		group, err := subscribe(s, i, v)
		if err != nil {
			time.Sleep(w)
			continue
		}
		var client net.Conn
		for i := 0; i < 5; i++ {
			if c, err := openClient(d, false); err != nil {
				time.Sleep(time.Second * time.Duration(i))
				continue
			} else {
				client = c
			}
		}
		if client == nil {
			return fmt.Errorf("connection to %s failed after 5 retries", d)
		}
		if c != nil {
			c.InsecureSkipVerify = true
			client = tls.Client(client, c)
		}
		disassemble(client, group, z)
		time.Sleep(w)
	}
	return nil
}

func disassemble(w io.WriteCloser, r io.ReadCloser, s int) error {
	defer func() {
		w.Close()
		r.Close()
	}()
	if s <= 0 {
		s = DefaultBufferSize
	}

	for {
		chunk := make([]byte, 8192)
		c, err := r.Read(chunk)
		if err != nil {
			return err
		}
		if c < s {
			if _, err := w.Write(chunk[:c]); err != nil {
				return err
			}
		} else {
			buf := bytes.NewBuffer(chunk[:c])
			for i, c := 0, 1+(len(chunk)/s); i < c; i++ {
				if _, err := w.Write(buf.Next(s)); err != nil {
					return err
				}
			}
		}
	}
}

func reassemble(w io.WriteCloser, r io.ReadCloser, s int) error {
	defer func() {
		w.Close()
		r.Close()
	}()
	if s <= 0 {
		s = DefaultBufferSize
	}
	var buf bytes.Buffer
	for {
		chunk := make([]byte, s)
		c, err := r.Read(chunk)
		if err != nil {
			return err
		}
		if c < s {
			buf.Write(chunk[:c])
			if _, err := io.Copy(w, &buf); err != nil {
				return err
			}
		} else {
			buf.Write(chunk)
		}
	}
}

func transmit(w io.WriteCloser, r io.ReadCloser, s int) error {
	defer func() {
		w.Close()
		r.Close()
	}()
	if s <= 0 {
		s = DefaultBufferSize
	}
	buf := make([]byte, s)
	for {
		if _, err := io.CopyBuffer(w, r, buf); err != nil && err == io.EOF {
			return err
		}
	}
	return nil
}

func subscribe(source, nic string, v bool) (net.Conn, error) {
	uri, err := url.Parse(source)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveUDPAddr(uri.Scheme, uri.Host)
	if err != nil {
		return nil, err
	}
	if !addr.IP.IsMulticast() {
		return nil, fmt.Errorf("%s not a multicast address", addr)
	}
	var ifi *net.Interface
	if i, err := net.InterfaceByName(nic); err == nil {
		ifi = i
	}

	c, err := net.ListenMulticastUDP(uri.Scheme, ifi, addr)
	if err != nil {
		return nil, fmt.Errorf("fail to subscribe to group %s: %s", uri.Host, err)
	}
	if v {
		return &conn{Conn: c}, nil
	} else {
		return c, nil
	}
}

func openClient(source string, v bool) (net.Conn, error) {
	uri, err := url.Parse(source)
	if err != nil {
		return nil, err
	}

	switch s := strings.ToLower(uri.Scheme); s {
	case "udp", "udp4", "udp6":
		raddr, err := net.ResolveUDPAddr(s, uri.Host)
		if err != nil {
			return nil, err
		}
		c, err := net.DialUDP(s, nil, raddr)
		if err != nil {
			return nil, err
		}
		if v {
			return &conn{Conn: c}, nil
		} else {
			return c, nil
		}
	case "tcp", "tcp4", "tcp6":
		raddr, err := net.ResolveTCPAddr(s, uri.Host)
		if err != nil {
			return nil, err
		}
		c, err := net.DialTCP(s, nil, raddr)
		if err != nil {
			return nil, err
		}
		if v {
			return &conn{Conn: c}, nil
		} else {
			return c, nil
		}
	case "unix":
		c, err := net.Dial(s, uri.Path)
		if err != nil {
			return nil, err
		}
		if v {
			return &conn{Conn: c}, nil
		} else {
			return c, nil
		}
	case "":
		return nil, fmt.Errorf("no protocol provided. choose between (udp|tcp)[46]")
	default:
		return nil, fmt.Errorf("unsupported protocol provided %s", s)
	}
}
