package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"hash/adler32"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"

	_ "github.com/midbel/transmit"
	"github.com/midbel/uuid"
)

var (
	ErrUnknownId   = errors.New("packet with unknown packet id received")
	ErrInvalidHash = errors.New("packet hash invalid")
)

type Conn struct {
	net.Conn
	Label []byte

	count uint32
	proxy bool
	pad   io.Reader
}

func Dial(a string, id []byte, c *tls.Config) (net.Conn, error) {
	w, err := net.Dial("tcp", a)
	if w, ok := w.(*net.TCPConn); ok {
		w.SetNoDelay(false)
	}
	if err != nil {
		return nil, err
	}
	if c != nil {
		w = tls.Client(w, c)
	}
	return &Conn{
		Conn:  w,
		Label: id,
		pad:   bufio.NewReaderSize(rand.Reader, 4096),
	}, nil
}

func (c *Conn) Write(d []byte) (int, error) {
	b := new(bytes.Buffer)

	b.Write(c.Label)
	binary.Write(b, binary.BigEndian, uint16(len(d)))
	binary.Write(b, binary.BigEndian, atomic.AddUint32(&c.count, 1))
	b.Write(d)
	if b.Len() < 512 {
		io.CopyN(b, c.pad, 512)
	}

	_, err := io.Copy(c.Conn, b)
	return len(d), err
}

func (c *Conn) Read(d []byte) (int, error) {
	b := make([]byte, len(d))
	if r, err := c.Conn.Read(b); err != nil {
		return r, err
	} else {
		b = b[:r]
	}
	s := binary.BigEndian.Uint16(b[16:18])
	if !bytes.Equal(b[:16], c.Label) {
		return 0, ErrUnknownId
	}
	return copy(d, b[:22+s]), nil
}

type Group struct {
	net.Conn
	Label []byte
}

func (g *Group) Read(b []byte) (int, error) {
	d := make([]byte, len(b))
	r, err := g.Conn.Read(d)
	if err != nil {
		return r, err
	}
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, adler32.Checksum(d[:r]))

	return copy(b, append(d[:r], sum...)), nil
}

func (g *Group) Write(b []byte) (int, error) {
	data, sum := b[22:len(b)-adler32.Size], b[len(b)-adler32.Size:]
	if a := adler32.Checksum(data); a != binary.BigEndian.Uint32(sum) {
		return 0, ErrInvalidHash
	}
	_, err := g.Conn.Write(data)
	return len(b), err
}

func (g *Group) UnmarshalJSON(b []byte) error {
	v := struct {
		Id     string `json:"id"`
		Addr   string `json:"addr"`
		Ifi    string `json:"interface"`
		Listen bool   `json:"listen"`
	}{}

	var err error
	if err = json.Unmarshal(b, &v); err != nil {
		return err
	}
	u, err := uuid.UUID5(uuid.URL, []byte(v.Id))
	if err != nil {
		return err
	}
	g.Label = u.Bytes()
	if v.Listen {
		a, err := net.ResolveUDPAddr("udp", v.Addr)
		if err != nil {
			return err
		}
		g.Conn, err = net.ListenMulticastUDP("udp", nil, a)
	} else {
		g.Conn, err = net.Dial("udp", v.Addr)
	}
	return err
}

type Config struct {
	Listen      bool   `json:"-"`
	Address     string `json:"gateway"`
	Verbose     bool   `json:"verbose"`
	Size        int    `json:"size"`
	Certificate *tls.Config

	Proxy string `json:"proxy"`

	Groups []*Group `json:"groups"`
}

func (c *Config) UnmarshalJSON(b []byte) error {
	z := struct {
		Location string `json:"location"`
		Insecure bool   `json:"insecure"`
		Server   string `json:"server"`
	}{}
	v := struct {
		Address     string      `json:"gateway"`
		Proxy       string      `json:"proxy"`
		Verbose     bool        `json:"verbose"`
		Size        int         `json:"size"`
		Groups      []*Group    `json:"groups"`
		Certificate interface{} `json:"certificate"`
	}{Certificate: &z}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	c.Address = v.Address
	c.Verbose = v.Verbose
	c.Proxy = v.Proxy
	c.Size = v.Size
	c.Groups = v.Groups

	if i, err := os.Stat(z.Location); err == nil && i.IsDir() {
		t := filepath.Join(z.Location, "transmit.cert")
		k := filepath.Join(z.Location, "transmit.pem")

		if cert, err := tls.LoadX509KeyPair(t, k); err != nil {
			return err
		} else {
			c.Certificate = &tls.Config{
				Certificates:       []tls.Certificate{cert},
				ClientAuth:         tls.RequireAnyClientCert,
				ServerName:         z.Server,
				InsecureSkipVerify: z.Insecure,
			}
		}
	}

	return nil
}

var config *Config

func init() {
	config = new(Config)

	flag.BoolVar(&config.Listen, "l", config.Listen, "listen")
	flag.BoolVar(&config.Verbose, "v", config.Verbose, "verbose")
	flag.Parse()

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(config); err != nil {
		log.Fatalln("bad configuration", err)
	}
	log.SetOutput(ioutil.Discard)
}

func main() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Kill, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sig
		cancel()
		log.Println("done transmitting! abort on signal...")
	}()
	log.SetPrefix("transmit: ")
	if config.Verbose {
		log.SetOutput(os.Stderr)
	}

	var err error
	switch {
	case config.Listen:
		err = distribute(ctx, *config)
	default:
		err = forward(ctx, *config)
	}
	if err != nil {
		log.Fatalln(err)
	}
}

func distribute(ctx context.Context, c Config) error {
	s, err := net.Listen("tcp", c.Address)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		s.Close()
	}()
	if c.Certificate != nil {
		s = tls.NewListener(s, c.Certificate)
	}
	log.Printf("listenning on %s", s.Addr())

	for {
		r, err := s.Accept()
		if err != nil {
			return err
		}

		id := make([]byte, 22)
		if _, err := io.ReadFull(r, id); err != nil {
			log.Printf("read transaction id from %s failed: %s", r.RemoteAddr(), err)
			r.Close()
			continue
		}
		id = id[:16]

		var w, x net.Conn
		for _, g := range c.Groups {
			if bytes.Equal(id, g.Label) {
				w = g
				break
			}
		}
		if _, _, err := net.SplitHostPort(c.Proxy); err == nil {
			var err error
			if c.Certificate != nil {
				x, err = tls.Dial("tcp", c.Proxy, c.Certificate)
			} else {
				x, err = net.Dial("tcp", c.Proxy)
			}
			if _, err = x.Write(id); err != nil {
				log.Printf("fail to initiate transaction with %s: %s", c.Proxy, err)
			}
			log.Printf("transaction initiated with %s: %x", c.Proxy, id)
		}
		if w == nil && x == nil {
			log.Println("abort current transaction")
			r.Close()
			continue
		}
		log.Printf("transaction initiated: %x", id)
		go transmit(ctx, &Conn{Conn: r, Label: id}, w, x)
	}

	return nil
}

func forward(ctx context.Context, c Config) error {
	for _, g := range c.Groups {
		w, err := Dial(config.Address, g.Label, c.Certificate)
		if err != nil {
			return err
		}
		log.Printf("transaction initiated: %x", g.Label)
		if _, err := w.Write([]byte{}); err != nil {
			return err
		}
		go transmit(ctx, g, w, nil)
	}
	<-ctx.Done()

	return nil
}

func transmit(ctx context.Context, r, w, p net.Conn) {
	go func() {
		<-ctx.Done()
		r.Close()
		w.Close()
		if p != nil {
			p.Close()
		}
	}()
	var c io.Reader
	if p != nil {
		c = io.TeeReader(r, p)
	} else {
		c = r
	}
	for {
		_, err := io.Copy(w, c)
		switch err {
		case io.EOF:
			return
		case nil:
		default:
			if err, ok := err.(net.Error); ok && !(err.Timeout() || err.Temporary()) {
				log.Printf("connection closed with remote host: %s", err)
				return
			}
			log.Printf("fail to transmit packet: %s", err)
		}
	}
}
