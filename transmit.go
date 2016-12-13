//package main implements a command to tunnel packets coming from a multicast
//stream over a TCP connection to another network in order to "recreate" the
//original stream.
package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	//"io"
	"io/ioutil"
	"log"
	"log/syslog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/midbel/etc"
)

const helpText = `%[1]s tunnel multicast packets over a TCP connection.

%[1]s is a small tool designed to send multicast packets from one network
to another which missed connectivity with the former.

Its original goal is to be able to forward packets from multicast groups
through firewalls that allow only outgoing TCP/TLS connections.

%[1]s can also be used to transfer files at regular interval in the same
way that it send packets.

Options:
  -h: show this help message and exit
  -l: listen for incoming packets
  -c: certificates to encrypt communication between agents
  -k: keep transferred file(s) (default: remove files transferred)
  -i: network interface to used when subscribing to a multicast group
  -o: address of syslog daemon
  -p: acts as a proxy. It does not try to reassemble chunk of the initial packet
  -m: ssl mode
  -s: max count of bytes to read from connections
  -t: transfer file(s)
  -v: dump packets length + md5 on stderr
  -w: time to wait when connection failure is encountered

Arguments:
  local: local address to listen for incoming packets
  remote: remote address to forward received packets

Usage: %[1]s [options] <local> <remote>
`

const (
	defaultBufferSize = 8192
	defaultChunkSize  = 1024
)

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
		log.Printf("%8d | %8d | %x\n", v, n, md5.Sum(b))
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
		log.Printf("%8d | %8d | %x\n", v, n, md5.Sum(b))
	}(b[:n])

	return n, err
}

func init() {
	name := filepath.Base(os.Args[0])
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, helpText, name)
		return
	}
	log.SetPrefix(fmt.Sprintf("[%s] ", name))
}

var logger *syslog.Writer

func main() {
	config := struct {
		Local       string
		Remote      string
		Log         string
		Listen      bool
		Verbose     bool
		Transfer    bool
		Keep        bool
		Proxy       bool
		Size        int
		Interface   string
		Certificate string
		Mode        string
		Wait        int
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
	flag.StringVar(&config.Log, "o", "", "syslog")
	flag.StringVar(&config.Interface, "i", "", "interface")
	flag.StringVar(&config.Certificate, "c", "", "certificate")
	flag.StringVar(&config.Mode, "m", "", "sslmode")
	flag.IntVar(&config.Wait, "w", config.Wait, "wait")
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
			log.Fatalf("fail to load certificate from %s: %s\n", config.Certificate, err)
		} else {
			cfg = &tls.Config{Certificates: []tls.Certificate{c}}
			switch l, m := config.Listen, config.Mode; {
			case l && m == "enforce":
				cfg.ClientAuth = tls.RequireAndVerifyClientCert
			case l && m == "require":
				cfg.ClientAuth = tls.RequireAnyClientCert
			case l && m == "disable":
				cfg.ClientAuth = tls.NoClientCert
			case l && m == "":
				cfg.ClientAuth = tls.RequestClientCert
			case !l && (m == "insecure" || m == ""):
				cfg.InsecureSkipVerify = true
			case !l && m == "strict":
				cfg.InsecureSkipVerify = false
			}
		}
	}
	var err error
	priority := syslog.LOG_USER | syslog.LOG_INFO
	if config.Log == "" {
		logger, err = syslog.New(priority, os.Args[0])
	} else {
		logger, err = syslog.Dial("udp", config.Log, priority, os.Args[0])
	}
	if err != nil {
		log.Fatalln(err)
	}
	defer logger.Close()

	switch {
	case config.Listen:
		err = runGateway(local, remote, config.Verbose, config.Proxy, cfg)
	case config.Transfer:
		wait := time.Duration(config.Wait) * time.Second
		err = runTransfer(local, remote, config.Size, config.Keep, config.Verbose, wait, cfg)
	default:
		wait := time.Duration(config.Wait) * time.Second
		err = runRelay(local, remote, config.Interface, config.Size, config.Verbose, wait, cfg)
	}
	if err != nil {
		log.Fatalln(err)
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
func runGateway(s, d string, v, p bool, c *tls.Config) error {
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
		listener = tls.NewListener(serv, c)
	} else {
		listener = serv
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	logger.Info(fmt.Sprintf("start listening for incoming connections %s", s))
	for {
		client, err := listener.Accept()
		if err != nil {
			logger.Err(fmt.Sprintf("error while receiving new connection: %s", err))
			client.Close()
			continue
		}
		group, err := openClient(d, v)
		if err != nil {
			logger.Err(fmt.Sprintf("error while opening connection to %s: %s", d, err))
			client.Close()
			continue
		}
		wg.Add(1)
		go func(c, g net.Conn) {
			defer wg.Done()
			if err := joinPackets(g, c, p); err != nil {
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
	if z <= 0 {
		z = defaultChunkSize
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
			var client net.Conn
			for i := 0; i < 5; i++ {
				if c, err := openClient(d, false); err != nil {
					logger.Err(fmt.Sprintf("error while opening connection with %s (attempt #%d): %s", d, i+1, err))
					time.Sleep(time.Second * time.Duration(i))
					continue
				} else {
					client = c
					logger.Info(fmt.Sprintf("connection to %s established (attempt #%d)", c.RemoteAddr(), i+1))
					break
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
				logger.Err(fmt.Sprintf("error while opening connection with %s (attempt #%d): %s", d, i+1, err))
				time.Sleep(time.Second * time.Duration(i*3))
				continue
			} else {
				client = c
				logger.Info(fmt.Sprintf("connection to %s established (attempt #%d)", c.RemoteAddr(), i+1))
				break
			}
		}
		if client == nil {
			return fmt.Errorf("connection to %s failed after 5 retries", d)
		}
		if c != nil {
			client = tls.Client(client, c)
		}
		copyPackets(client, group, z)
		time.Sleep(w)
	}
}

func copyFiles(c net.Conn, s string, v, k bool, t time.Time, w time.Duration, split bufio.SplitFunc) error {
	defer func() {
		c.Close()
		logger.Info(fmt.Sprintf("connection closed with %s", c.RemoteAddr()))
	}()
	infos, err := ioutil.ReadDir(s)
	if err != nil {
		logger.Err(fmt.Sprintf("error while scanning directory %s: %s", s, err))
		return err
	}
	var total, counter uint64
	for _, i := range infos {
		f, err := os.Open(filepath.Join(s, i.Name()))
		if err != nil {
			logger.Err(fmt.Sprintf("error while opening %s: %s", i.Name(), err))
			continue
		}
		i, err := f.Stat()
		if err != nil {
			continue
		}
		if t.Sub(i.ModTime()) <= w {
			continue
		}
		total += uint64(i.Size())
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
				logger.Err(fmt.Sprintf("error while sending chunk to %s: %s", c.RemoteAddr(), err))
				break
			}
		}
		f.Close()
		if !k && err == nil {
			err := os.Remove(f.Name())
			logger.Err(fmt.Sprintf("error while remoing %s: %s", f.Name(), err))
		}
		if v {
			go log.Printf("%8d | %8d | %x\n", counter, size, sum.Sum(nil))
		}
	}
	logger.Info(fmt.Sprintf("done sending %d files to %s (%d bytes)", counter, c.RemoteAddr(), total))
	return nil
}

func joinPackets(w net.Conn, r net.Conn, p bool) error {
	defer func() {
		w.Close()
		r.Close()
		logger.Info(fmt.Sprintf("done transmitting packets from %s to %s", r.LocalAddr(), w.RemoteAddr()))
	}()
	scan := bufio.NewScanner(r)
	scan.Split(scanPackets)
	
	logger.Info(fmt.Sprintf("start sending packets from %s to %s", r.LocalAddr(), w.RemoteAddr()))
	for scan.Scan() {
		if _, err := w.Write(scan.Bytes()); err != nil {
			logger.Err(fmt.Sprintf("error while sending packet to %s: %s", w.RemoteAddr(), err))
			return err
		}
	}
	if err := scan.Err(); err != nil {
		logger.Err(fmt.Sprintf("error while scanning packet from %s: %s", r.RemoteAddr(), err))
		return err
	}
	return nil
}

//copyPackets read packets from r before copying them to w without touching
//them. It reads up to c bytes from r. An error is returned on any error
//occurring during reading and writing.
func copyPackets(w net.Conn, r net.Conn, z int) error {
	defer func() {
		w.Close()
		r.Close()
		logger.Info(fmt.Sprintf("done transmitting packets from %s to %s", r.LocalAddr(), w.RemoteAddr()))
	}()
	if z <= 0 {
		z = defaultBufferSize
	}
	chunk := make([]byte, z)
	
	var buf bytes.Buffer
	logger.Info(fmt.Sprintf("start copying packets from %s to %s", r.LocalAddr(), w.RemoteAddr()))
	for {
		c, err := r.Read(chunk)
		if err != nil {
			logger.Err(fmt.Sprintf("error while reading packet from %s: %s", r.RemoteAddr(), err))
			return err
		}
		data := make([]byte, c)
		copy(data, chunk)
		
		sum := md5.Sum(data)
		binary.Write(&buf, binary.BigEndian, uint32(md5.Size + c))
		buf.Write(sum[:])
		buf.Write(data)
		
		if _, err := w.Write(buf.Bytes()); err != nil {
			logger.Err(fmt.Sprintf("error while sending packet to %s: %s", w.RemoteAddr(), err))
			return err
		}
		buf.Reset()
	}
}

func scanPackets(chunk []byte, ateof bool) (int, []byte, error) {
	if len(chunk) < 4+md5.Size {
		return 0, nil, nil
	}
	length := int(binary.BigEndian.Uint32(chunk[:4]))
	if length > len(chunk) {
		return 0, nil, nil
	}

	data := make([]byte, length-md5.Size)
	copy(data, chunk[4+md5.Size:])
	
	sum := md5.Sum(data)
	if !bytes.Equal(chunk[4:20], sum[:]) {
		return 0, nil, fmt.Errorf("checksum are not equals (%x != %x)", data[:md5.Size], sum)
	}

	return length+4, data, nil
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
