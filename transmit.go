package main

import (
	"bufio"
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

	"github.com/midbel/etc"
)

const helpText = `{{.Name}} tunnel multicast packets over a TCP connection.

{{.Name}} is a small tool designed to send multicast packets from one network
to another which missed connectivity with the former.

Its original goal is to be able to forward packets from multicast groups
through firewalls that allow only outgoing TCP/TLS connections.

{{.Name}} can also be used to transfer files at regular interval in the same
way that it send packets.

options:
	
	-l: listen for incoming packets
	-c: certificates to encrypt communication between agents
	-k: keep transferred file(s) (default: remove files transferred)
	-w: time to wait when connection failure is encountered
	-s: size of bytes to read/write from connections
	-t: transfer file(s)
	-v: dump packets length + md5 on stderr

arguments:
	
	local: local address to listen for incoming packets
	remote: remote address to forward received packets

`

const defaultBufferSize = 1024

//conn is a convenient type to dump debuging information by embeding a net.Conn
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
		fmt.Fprintf(os.Stderr, "usage: %s [-p] [-t] [-k] [-] [-c] [-w] <local> <remote>\n", os.Args[0])
		return
	}
}

func main() {
	config := struct {
		Local       string `ini:"transmit>local"`
		Remote      string `ini:"transmit>remote"`
		Listen      bool   `ini:"transmit>listen"`
		Verbose     bool   `ini:"transmit>verbose"`
		Transfer    bool   `ini:"transmit>transfer"`
		Keep        bool   `ini:"transmit>keep"`
		Proxy       bool   `ini:"transmit>proxy"`
		Size        int    `ini:"transmit>size"`
		Interface   string `ini:"transmit>interface"`
		Certificate string `ini:"transmit>certificate"`
		Wait        int    `ini:"transmit>wait"`
	}{}

	if err := etc.Configure(&config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	flag.IntVar(&config.Size, "s", defaultBufferSize, "size")
	flag.BoolVar(&config.Verbose, "v", false, "verbose")
	flag.BoolVar(&config.Listen, "l", false, "listen")
	flag.BoolVar(&config.Keep, "k", false, "keep")
	flag.BoolVar(&config.Transfer, "t", false, "transfer")
	flag.BoolVar(&config.Proxy, "p", false, "proxy")
	flag.StringVar(&config.Interface, "i", "eth0", "interface")
	flag.StringVar(&config.Certificate, "c", "", "certificate")
	flag.IntVar(&config.Wait, "w", config.Wait, "")
	flag.Parse()

	var local, remote string
	switch flag.NArg() {
	case 0:
		local, remote = config.Local, config.Remote
	case 2:
		local, remote = flag.Arg(0), flag.Arg(1)
	default:
		flag.Usage()
		os.Exit(1)
	}
	if local == "" || remote == "" {
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
	switch {
	case config.Listen:
		err = runGateway(local, remote, config.Size, config.Verbose, config.Proxy, cfg)
	case config.Transfer:
		wait := time.Duration(config.Wait) * time.Second
		err = runTransfer(local, remote, config.Size, config.Keep, config.Verbose, wait, cfg)
	default:
		wait := time.Duration(config.Wait) * time.Second
		err = runRelay(local, remote, config.Interface, config.Size, config.Verbose, wait, cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

//Starts transmit in listen mode (if c is given, transmit will listen for TLS/
//SSL connections. It will listen on s for incoming packets and  re-sent them
//to d. If p is specified, transmit will run as proxy and won't try to re-
//assemble packets. If not, it will use z as the size of the packets in order
//to re-assemble the original packet.
//
//If v is given, transmit will dump on stderr a timestamp, a counter, the size
//of the ressambled packets and its md5 sum.
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
		switch uri.Query().Get("sslmode") {
		case "enforce":
			c.ClientAuth = tls.RequireAndVerifyClientCert
		case "require":
			c.ClientAuth = tls.RequireAnyClientCert
		case "disable":
			c.ClientAuth = tls.NoClientCert
		default:
			c.ClientAuth = tls.RequestClientCert
		}
		listener = tls.NewListener(serv, c)
	} else {
		listener = serv
	}

	var wg sync.WaitGroup
	defer wg.Wait()
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
			if err := reassemble(g, c, z, p); err != nil {
				return
			}
		}(client, group)
	}
}

//Starts transmit to transfer at regular interval (specified by w) files stored
//into s to d. If k is given and true, transmit will keep the files into s
//otherwise, it will delete them from s (this is the prefer way of working).
//
//If v is given, transmit will dump on stderr a timestamp, a counter, the size
//of the ressambled packets and its md5 sum.
func runTransfer(s, d string, z int, k bool, v bool, w time.Duration, c *tls.Config) error {
	if i, err := os.Stat(s); err != nil || !i.IsDir() {
		return fmt.Errorf("%s not a directory", i.Name())
	}
	var client net.Conn

	if z <= 0 {
		z = defaultBufferSize
	}

	split := func(buf []byte, ateof bool) (int, []byte, error) {
		if ateof && len(buf) > 0 {
			return len(buf), buf[:], nil
		}
		if len(buf) < z {
			return 0, nil, nil
		}
		return z, buf[:z], nil
	}

	t := time.NewTicker(w)
	defer t.Stop()

	sema := make(chan struct{}, 1)
	for t := range t.C {
		select {
		case sema <- struct{}{}:
			for i := 0; i < 5; i++ {
				if c, err := openClient(d, false); err != nil {
					time.Sleep(time.Second * time.Duration(i))
					continue
				} else {
					client = c
				}
			}
			if client == nil {
				continue
			}
			if c != nil {
				c.InsecureSkipVerify = true
				client = tls.Client(client, c)
			}
			copyFiles(client, s, v, k, t, w, split)
			<-sema
		case <-time.After(time.Millisecond * 3):
			continue
		}
	}
	return nil
}

//Starts transmit to subscrive to a mutlicast group s (via network interface i)
//and send reveived packets of size z to d. If the connection with d is lost, 
//runRelay will wait w seconds before trying to reconnect to d.
//
//To establish a connection with d, runRelay will try 5 times and abort if no
//connection can be successfully established with d. Between each trial, a pause
//of some seconds will be made.
//
//If v is given, transmit will dump on stderr a timestamp, a counter, the size
//of the ressambled packets and its md5 sum.
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
				time.Sleep(time.Second * time.Duration(i*3))
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
}

func copyFiles(c net.Conn, s string, v, k bool, t time.Time, w time.Duration, split bufio.SplitFunc) error {
	defer c.Close()
	infos, err := ioutil.ReadDir(s)
	if err != nil {
		return err
	}
	var counter uint64
	for _, i := range infos {
		f, err := os.Open(filepath.Join(s, i.Name()))
		if err != nil {
			continue
		}
		i, err := f.Stat()
		if err != nil {
			continue
		}
		if t.Sub(i.ModTime()) <= w {
			continue
		}
		s := bufio.NewScanner(f)
		s.Split(split)

		sum := md5.New()
		counter++

		var size int
		for s.Scan() {
			if err = s.Err(); err != nil {
				break
			}
			buf := s.Bytes()
			size += len(buf)
			sum.Write(buf)
			if _, err = c.Write(buf); err != nil {
				break
			}
		}
		f.Close()
		if !k && err == nil {
			os.Remove(f.Name())
		}
		if v {
			go fmt.Fprintf(os.Stderr, "%s | %8d | %8d | %x\n", t.Format(time.RFC3339), counter, size, sum.Sum(nil))
		}
	}
	return nil
}

func disassemble(w net.Conn, r net.Conn, s int) error {
	defer func() {
		w.Close()
		r.Close()
	}()
	if s <= 0 {
		s = defaultBufferSize
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

func reassemble(w net.Conn, r net.Conn, s int, p bool) error {
	defer func() {
		w.Close()
		r.Close()
	}()
	if s <= 0 {
		s = defaultBufferSize
	}
	var (
		buf   bytes.Buffer
		abort bool
	)
	for {
		chunk := make([]byte, s)
		c, err := r.Read(chunk)
		switch {
		case err == io.EOF:
			abort = true
		case err != nil:
			return err
		}

		if c < s || p {
			buf.Write(chunk[:c])
			if _, err := io.Copy(w, &buf); err != nil && err != io.EOF {
				return err
			}
			if abort {
				return io.EOF
			}
		} else {
			buf.Write(chunk)
		}
	}
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
	}
	return c, nil
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
		}
		return c, nil
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
		}
		return c, nil
	case "unix":
		c, err := net.Dial(s, uri.Path)
		if err != nil {
			return nil, err
		}
		if v {
			return &conn{Conn: c}, nil
		}
		return c, nil
	case "":
		return nil, fmt.Errorf("no protocol provided. choose between (udp|tcp)[46]")
	default:
		return nil, fmt.Errorf("unsupported protocol provided %s", s)
	}
}
